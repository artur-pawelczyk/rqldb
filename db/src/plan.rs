use crate::db::SharedObject;
use crate::Cell;
use crate::Database;
use crate::Operator;
use crate::dsl;
use crate::object::Attribute;
use crate::object::NamedAttribute as _;
use crate::schema::AttributeRef;
use crate::schema::Column;
use crate::schema::{Schema, Relation, Type};
use crate::tuple::Tuple;

use std::collections::BTreeMap;
use std::collections::HashMap;
use std::rc::Rc;

type Result<T> = std::result::Result<T, String>;    

#[derive(Default)]
pub(crate) struct Plan<'schema> {
    pub source: Source,
    pub filters: Vec<Filter>,
    pub joins: Vec<Join<'schema>>,
    pub finisher: Finisher<'schema>,
}

impl<'schema> Plan<'schema> {
    #[cfg(test)]
    pub fn insert(obj: &SharedObject, values: &[&str]) -> Self {
        let attributes: Vec<_> = obj.borrow().attributes().cloned().collect();
        let contents = std::iter::zip(attributes.iter(), values.iter())
            .map(|(a, s)| (a.clone(), String::from(*s)))
            .collect();

        Self{
            source: Source::Tuple(contents),
            finisher: Finisher::Insert(Rc::clone(obj)),
            ..Plan::default()
        }
   }

    #[cfg(test)]
    pub fn scan(obj: &SharedObject) -> Self {
        Self{
            source: Source::scan_table(obj),
            finisher: Finisher::Return,
            ..Plan::default()
        }
    }

    pub fn final_attributes(&self) -> Vec<Attribute> {
        if let Some(last_join) = self.joins.iter().last() {
            last_join.attributes_after().collect()
        } else {
            self.source.attributes()
        }
    }
}

pub(crate) struct Filter {
    attribute: Attribute,
    right: Vec<u8>,
    comp: &'static dyn Fn(&[u8], &[u8]) -> bool,
}

impl Filter {
    pub fn matches_tuple(&self, tuple: &Tuple) -> bool {
        let cell = tuple.element(&self.attribute).unwrap();
        let left = cell.bytes();
        (self.comp)(left, &self.right)
    }
}

pub(crate) struct Join<'a> {
    joinee: &'a Relation,
    joiner: &'a Relation,
    joinee_key: AttributeRef,
    joiner_key: AttributeRef,
}

impl<'a> Join<'a> {
    pub fn source_table(&self) -> &Relation {
        self.joiner
    }

    pub fn joinee_key(&self) -> &AttributeRef {
        &self.joinee_key
    }

    pub fn joiner_key(&self) -> &AttributeRef {
        &self.joiner_key
    }

    pub fn attributes_after(&'a self) -> impl Iterator<Item = Attribute> + 'a {
        self.joinee.attributes()
            .chain(self.joiner.attributes())
            .map(Attribute::from)
    }
}

#[derive(Default)]
pub(crate) enum Source {
    TableScan(SharedObject),
    Tuple(BTreeMap<Attribute, String>),
    ReferencedTuple(SharedObject, HashMap<AttributeRef, String>),
    IndexScan(SharedObject, Cell),
    #[default]
    Nil,
}

impl Source {
    pub fn attributes(&self) -> Vec<Attribute> {
        match &self {
            Self::Tuple(values) => values.keys().cloned().collect(),
            Self::ReferencedTuple(obj, _) => obj.borrow().attributes().cloned().collect(),
            Self::TableScan(obj) => obj.borrow().attributes().cloned().collect(),
            Self::IndexScan(obj, _) => obj.borrow().attributes().cloned().collect(),
            _ => vec![]
        }
    }
}

enum QuerySource<'q> {
    Tuple(HashMap<&'q str, (Type, &'q str)>),
    Scan(&'q str),
    IndexScan(&'q str, Cell),
}

impl<'q> QuerySource<'q> {
    fn into_source(self, db: &Database) -> Result<Source> {
        match self {
            Self::Tuple(values) => Ok(Source::from_map(&values)),
            Self::Scan(name) => {
                let obj = db.object(name).ok_or_else(|| format!("No such relation {name}"))?;
                Ok(Source::scan_table(&obj))
            },
            Self::IndexScan(attr_name, cell) => {
                let attr = db.schema().find_column(attr_name).ok_or_else(|| format!("No such attribute {attr_name}"))?;
                let obj = db.object(&attr.reference()).expect("Attribute found so the object must exist");
                if attr.indexed() {
                    Ok(Source::scan_index(&obj, cell))
                } else {
                    Err(format!("Attribute {attr_name} is not an index"))
                }
            },
        }
    }
}

impl Source {
    fn scan_table(obj: &SharedObject) -> Self {
        Source::TableScan(Rc::clone(obj))
    }

    fn from_map(map: &HashMap<&str, (Type, &str)>) -> Self {
        let mut attributes = Vec::new();
        let mut values = BTreeMap::new();

        for (pos, (k, v)) in map.iter().enumerate() {
            let attr = Attribute { pos, name: Box::from(*k), kind: v.0, reference: None };
            attributes.push(attr.clone());
            values.insert(attr, v.1.to_string());
        }

        Self::Tuple(values)
    }

    fn scan_index(obj: &SharedObject, val: Cell) -> Self {
        Self::IndexScan(Rc::clone(obj), val)
    }
}

#[derive(Default, Debug)]
pub(crate) enum Finisher<'a> {
    Return,
    Insert(SharedObject),
    Count,
    Apply(ApplyFn, Column<'a>),
    Delete(&'a Relation),
    #[default]
    Nil,
}

impl<'a> Finisher<'a> {
    fn apply(name: &str, attr: Column<'a>) -> Self {
        Finisher::Apply(ApplyFn::from(name), attr)
    }
}

#[derive(Default, PartialEq, Eq, Debug)]
pub enum ApplyFn {
    #[default]
    NoOp,
    Sum, Max
}

impl ApplyFn {
    fn from(s: &str) -> Self {
        match s {
            "sum" => Self::Sum,
            "max" => Self::Max,
            _ => Self::NoOp,
        }
    }
}

pub(crate) fn compute_plan<'schema>(db: &'schema Database, query: &dsl::Query) -> Result<Plan<'schema>> {
    let source = compute_source(&query.source)?;
    let plan = compute_finisher(source, db, query)?;
    let plan = compute_joins(plan, db.schema(), query)?;
    let plan = compute_filters(plan, query)?;

    Ok(plan)
}

fn compute_source<'query>(dsl_source: &'query dsl::Source) -> Result<QuerySource<'query>> {
    match dsl_source {
        dsl::Source::Tuple(values) => {
            let tuple = values.iter().map(|attr| {
                let name = attr.name.as_ref();
                let kind: Type = attr.kind.into();
                let value = attr.value;
                (name, (kind, value))
            }).collect();
            Ok(QuerySource::Tuple(tuple))
        },
        dsl::Source::TableScan(relation_name) => {
            Ok(QuerySource::Scan(relation_name))
        },
        dsl::Source::IndexScan(index, Operator::EQ, val) => {
            Ok(QuerySource::IndexScan(index, Cell::from_string(val)))
        },
        _ => Err(String::from("Source not supported"))
    }
}

fn compute_filters<'a>(plan: Plan<'a>, query: &dsl::Query) -> Result<Plan<'a>> {
    if query.filters.is_empty() {
        return Ok(plan);
    }

    let attributes = plan.final_attributes();
    let mut filters = Vec::with_capacity(query.filters.len());
    for dsl_filter in &query.filters {
        if let Some(attr) = attributes.iter().find(|attr| attr == &filter_left(dsl_filter)) {
            if attr.kind == Type::NONE {
                return Err(format!("Cannot filter untyped attribute {}", attr.name()));
            }

            let op = filter_operator(dsl_filter);
            filters.push(Filter{
                attribute: attr.clone(),
                right: right_as_cell(attr.kind(), dsl_filter),
                comp: match op {
                    dsl::Operator::EQ => &|a, b| a == b,
                    dsl::Operator::GT => &|a, b| a > b,
                    dsl::Operator::GE => &|a, b| a >= b,
                    dsl::Operator::LT => &|a, b| a < b,
                    dsl::Operator::LE => &|a, b| a <= b,
                }
            });
        } else {
            return Err(format!("Attribute not found: {}", filter_left(dsl_filter)));
        }
    }

    Ok(Plan{ filters, ..plan })
}

fn compute_finisher<'query, 'schema>(source: QuerySource<'query>, db: &'schema Database, query: &'query dsl::Query) -> Result<Plan<'schema>> {
    match &query.finisher {
        dsl::Finisher::Insert(name) => {
            if let QuerySource::Tuple(input_map) = source {
                let relation = db.schema().find_relation(*name).ok_or("No such target table")?;
                let obj = db.object(*name).ok_or_else(|| format!("Target relation {name} not found"))?;
                let finisher = Finisher::Insert(Rc::clone(&obj));
                let mut values = HashMap::new();
                for attr in relation.attributes() {
                    if let Some((_, val)) = input_map.get(attr.name()) {
                        values.insert(attr.reference(), val.to_string());
                    } else if let Some((_, val)) = input_map.get(attr.short_name()) {
                        values.insert(attr.reference(), val.to_string());
                    } else {
                        return Err(format!("Missing attribute in source {}", attr.name()));
                    }

                }
                let source = Source::ReferencedTuple(Rc::clone(&obj), values);
                Ok(Plan { source, finisher, ..Default::default() })
            } else {
                panic!()
            }
        },
        dsl::Finisher::AllColumns => Ok(Plan{ finisher: Finisher::Return, source: source.into_source(db)?, ..Default::default() }),
        dsl::Finisher::Count => Ok(Plan{ finisher: Finisher::Count, source: source.into_source(db)?, ..Default::default() }),
        dsl::Finisher::Apply(f, args) => {
            let finisher = args
                .first()
                .and_then(|n| db.schema().find_column(n))
                .map(|attr| Ok(Finisher::apply(f, attr)))
                .unwrap_or(Err("apply: No such attribute"))?;
            Ok(Plan{ finisher, source: source.into_source(db)?, ..Default::default() })
        },
        dsl::Finisher::Delete => {
            let finisher = match &query.source {
                dsl::Source::TableScan(name) => db.schema().find_relation(*name).map(Finisher::Delete).ok_or("No such relation found for delete operation"),
                _ => Err("Illegal delete operation"),
            }?;
            Ok(Plan{ finisher, source: source.into_source(db)?, ..Default::default() })
        }
        _ => todo!(),
    }
}

fn right_as_cell(kind: Type, dsl_filter: &dsl::Filter) -> Vec<u8> {
    let right = match dsl_filter {
        dsl::Filter::Condition(_, _, right) => right
    };

    match kind {
        Type::NUMBER => right.parse::<i32>().map(|n| n.to_be_bytes().to_vec()).unwrap(),
        Type::TEXT => right.as_bytes().to_vec(),
        Type::BOOLEAN => if right == &"true" { vec![1] } else if right == &"false" { vec![0] } else { panic!() },
        Type::NONE => vec![],
        _ => panic!("unknown type: {}", kind),
    }
}

fn filter_operator(filter: &dsl::Filter) -> dsl::Operator {
    match filter {
        dsl::Filter::Condition(_, op, _) => *op
    }
}

fn filter_left<'a>(filter: &'a dsl::Filter) -> &'a str {
    match filter {
        dsl::Filter::Condition(left, _, _) => left
    }
}

fn compute_joins<'a>(plan: Plan<'a>, schema: &'a Schema, query: &dsl::Query) -> Result<Plan<'a>> {
    if query.join_sources.is_empty() {
        return Ok(plan);
    }

    let mut joins = Vec::with_capacity(query.join_sources.len());
    let joinee_table = match &query.source {
        dsl::Source::TableScan(name) => Some(name),
        _ => todo!(),
    }.and_then(|name| schema.find_relation(*name)).ok_or("No such table")?;

    for join_source in &query.join_sources {
        let joiner_table = match schema.find_relation(join_source.table) {
            Some(table) => table,
            None => return Err(format!("{} relation not found", join_source.table))
        };

        if let Some(joinee_key) = joinee_table.lookup(join_source.left).map(|c| c.reference()) {
            if let Some(joiner_key) = joiner_table.lookup(join_source.right).map(|c| c.reference()) {
                joins.push(Join {
                    joiner: joiner_table,
                    joinee: joinee_table,
                    joinee_key, joiner_key
                })
            } else {
                return Err(format!("Joiner: {} attribute not found", join_source.right))
            }
        } else {
            return Err(format!("Joinee: {} attribute not found", join_source.left))
        }
    }

    Ok(Plan{ joins, ..plan })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dsl::{AttrKind, TupleBuilder};
    use crate::dsl::Operator::{EQ, GT};
    use crate::schema::Type;
    use crate::tuple::PositionalAttribute as _;
    use crate::Command;


    #[test]
    fn test_compute_filter() {
        let mut db = Database::default();
        let command = Command::create_table("example")
            .column("id", Type::NUMBER)
            .column("content", Type::TEXT)
            .column("type", Type::NUMBER);
        db.execute_create(&command);

        let filters = expect_filters(compute_plan(&db, &dsl::Query::scan("example").filter("example.id", EQ, "1").filter("example.type", EQ, "2")), 2);
        assert_eq!(filters.get(0).map(|x| x.attribute.pos()), Some(0));
        assert_eq!(filters.get(1).map(|x| x.attribute.pos()), Some(2));

        let query = dsl::Query::scan("example").filter("not-a-column", EQ, "0");
        let failure = compute_plan(&db, &query);
        assert!(failure.is_err());
    }

    #[test]
    fn test_apply_filter() {
        let mut db = Database::default();
        db.execute_create(&Command::create_table("example")
                          .column("id", Type::NUMBER)
                          .column("content", Type::TEXT)
                          .column("type", Type::NUMBER));

        let attributes: Vec<Attribute> = db.object("example").unwrap().borrow().attributes().cloned().collect();
        let tuple_1 = tuple(&[(Type::NUMBER, "1"), (Type::TEXT, "content1"), (Type::NUMBER, "11")]);
        let tuple_2 = tuple(&[(Type::NUMBER, "2"), (Type::TEXT, "content2"), (Type::NUMBER, "12")]);

        let id_filter = expect_filter(compute_plan(&db, &dsl::Query::scan("example").filter("example.id", EQ, "1")));
        assert!(id_filter.matches_tuple(&Tuple::from_bytes(&tuple_1, &attributes)));
        assert!(!id_filter.matches_tuple(&Tuple::from_bytes(&tuple_2, &attributes)));

        let content_filter = expect_filter(compute_plan(&db, &dsl::Query::scan("example").filter("example.content", EQ, "content1")));
        assert!(content_filter.matches_tuple(&Tuple::from_bytes(&tuple_1, &attributes)));
        assert!(!content_filter.matches_tuple(&Tuple::from_bytes(&tuple_2, &attributes)));

        let comp_filter = expect_filter(compute_plan(&db, &dsl::Query::scan("example").filter("example.id", GT, "1")));
        assert!(!comp_filter.matches_tuple(&Tuple::from_bytes(&tuple_1, &attributes)));
        assert!(comp_filter.matches_tuple(&Tuple::from_bytes(&tuple_2, &attributes)));
    }

    fn expect_filters(result: Result<Plan>, n: usize) -> Vec<Filter> {
        match result {
            Ok(plan) => {
                assert_eq!(plan.filters.len(), n);
                plan.filters
            },
            Err(msg) => panic!("{}", msg)
        }
    }

    fn expect_filter(result: Result<Plan>) -> Filter {
        let filters = expect_filters(result, 1);
        filters.into_iter().next().unwrap()
    }

    #[test]
    fn test_compute_join() {
        let mut db = Database::default();
        db.execute_create(&Command::create_table("document")
                          .column("name", Type::TEXT)
                          .column("type_id", Type::NUMBER));
        db.execute_create(&Command::create_table("type")
                          .column("id", Type::NUMBER)
                          .column("name", Type::TEXT));

        let query = dsl::Query::scan("document").join("type", "document.type_id", "type.id");
        let joins = expect_join(compute_plan(&db, &query));
        assert_eq!(joins.source_table().name.as_ref(), "type");
        assert_eq!(attribute_names(&joins.attributes_after().collect::<Vec<_>>()), vec!["document.name", "document.type_id", "type.id", "type.name"]);

        let query = dsl::Query::scan("nothing").join("type", "a", "b");
        let missing_source_table = compute_plan(&db, &query);
        assert!(missing_source_table.is_err());

        let query = dsl::Query::scan("document").join("nothing", "a", "b");
        let missing_table = compute_plan(&db, &query);
        assert!(missing_table.is_err());

        let query = dsl::Query::scan("document").join("type", "something", "else");
        let missing_column = compute_plan(&db, &query);
        assert!(missing_column.is_err());
    }

    #[test]
    fn test_plan_multiple_joins() {
        let mut schema = Schema::default();
        schema.create_table("document").column("name", Type::TEXT).column("type_id", Type::NUMBER).column("author", Type::TEXT).add();
        schema.create_table("type").column("id", Type::TEXT).column("name", Type::NUMBER).add();
        schema.create_table("author").column("username", Type::TEXT).column("displayname", Type::TEXT).add();
        let db = Database::with_schema(schema);

        let query = dsl::Query::scan("document")
            .join("type", "document.type_id", "type.id")
            .join("author", "document.author", "author.username");
        let joins = expect_joins(compute_plan(&db, &query), 2);
        assert_eq!(joins[0].joiner.name.as_ref(), "type");
        assert_eq!(joins[1].joiner.name.as_ref(), "author");
    }

    #[test]
    fn test_filter_after_join() {
        let mut schema = Schema::default();
        schema.create_table("document").column("name", Type::TEXT).column("type_id", Type::NUMBER).add();
        schema.create_table("type").column("id", Type::NUMBER).column("name", Type::TEXT).add();
        let db = Database::with_schema(schema);

        let filter = expect_filter(compute_plan(&db, &dsl::Query::scan("document").join("type", "document.type_id", "type.id").filter("type.name", EQ, "b")));
        assert_eq!(filter.attribute.name(), "type.name");
    }

    fn expect_joins<'a>(result: Result<Plan<'a>>, n: usize) -> Vec<Join<'a>> {
        let plan = result.unwrap();
        assert_eq!(plan.joins.len(), n);
        plan.joins
    }

    fn expect_join<'a>(result: Result<Plan<'a>>) -> Join<'a> {
        expect_joins(result, 1).into_iter().next().unwrap()
    }

    #[test]
    fn test_filter_tuple() {
        let db = Database::default();
        let query = dsl::Query::tuple(&[("id", "1"), ("value", "some text")]).filter("id", Operator::EQ, "1");
        let result = compute_plan(&db, &query);
        assert!(result.is_err());
        assert_eq!(result.err(), Some(String::from("Cannot filter untyped attribute id")));

        let query = dsl::Query::tuple(
            TupleBuilder::new()
                .typed(AttrKind::Number, "id", "1")
                .typed(AttrKind::Text, "name", "some text")
        ).filter("id", Operator::EQ, "1");
        let result = compute_plan(&db, &query);

        let filter = expect_filter(result);
        assert_eq!(filter.attribute.name.as_ref(), "id");
    }

    #[test]
    fn test_source_types_for_select() {
        let mut schema = Schema::default();
        schema.create_table("example").column("id", Type::NUMBER).column("content", Type::TEXT).column("type", Type::NUMBER).add();
        let db = Database::with_schema(schema);

        let query = dsl::Query::scan("example");
        let result = compute_plan(&db, &query).unwrap();
        assert_eq!(source_attributes(&result.source), vec![Type::NUMBER, Type::TEXT, Type::NUMBER]);
        assert_eq!(attribute_names(&result.final_attributes()), vec!["example.id", "example.content", "example.type"]);
    }

    #[test]
    fn test_source_types_for_insert() {
        let mut schema = Schema::default();
        schema.create_table("example").column("id", Type::NUMBER).column("content", Type::TEXT).column("title", Type::TEXT).add();
        let db = Database::with_schema(schema);

        let query = dsl::Query::tuple(&[("id", "1"), ("content", "the-content"), ("title", "title")]).insert_into("example");
        let result = compute_plan(&db, &query).unwrap();
        assert_eq!(source_attributes(&result.source), vec![Type::NUMBER, Type::TEXT, Type::TEXT]);
        assert_eq!(attribute_names(&result.final_attributes()), vec!["example.id", "example.content", "example.title"]);

        let query = dsl::Query::tuple(&[("id", "1"), ("content", "the-content")]).insert_into("example");
        let wrong_tuple_len = compute_plan(&db, &query);
        assert!(wrong_tuple_len.is_err());
    }

    #[test]
    fn test_compute_finisher() {
        let mut schema = Schema::default();
        schema.create_table("example").column("id", Type::NUMBER).column("content", Type::TEXT).add();
        let db = Database::with_schema(schema);

        let query = dsl::Query::scan("example");
        let finisher = compute_plan(&db, &query)
            .unwrap().finisher;
        assert!(matches!(finisher, Finisher::Return));

        let query = dsl::Query::tuple(&[("id", "1"), ("content", "the content")]).insert_into("example");
        let finisher = compute_plan(&db, &query)
            .unwrap().finisher;
        assert!(matches!(finisher, Finisher::Insert{..}));

        let query = dsl::Query::scan("example").count();
        let finisher = compute_plan(&db, &query)
            .unwrap().finisher;
        assert!(matches!(finisher, Finisher::Count));

        let query = dsl::Query::scan("example").apply("sum", &["example.id"]);
        let finisher = compute_plan(&db, &query)
            .unwrap().finisher;
        assert!(matches!(finisher, Finisher::Apply(ApplyFn::Sum, _)));

        let query = dsl::Query::tuple(&[("id", "1")]).insert_into("not-a-table");
        let failure = compute_plan(&db, &query);
        assert!(failure.is_err());
    }

    #[test]
    fn test_match_types_for_insert() {
        let mut schema = Schema::default();
        schema.create_table("example").column("id", Type::NUMBER).column("content", Type::TEXT).add();
        let db = Database::with_schema(schema);

        let query = dsl::Query::tuple(&[("id", "1"), ("content", "aaa")]).insert_into("example");
        let plan = compute_plan(&db, &query).unwrap();

        assert!(matches!(plan.source.attributes()[0], Attribute { kind: Type::NUMBER, .. }));
        assert!(matches!(plan.source.attributes()[1], Attribute { kind: Type::TEXT, .. }));
    }

    #[test]
    fn test_scan_index() {
        let mut schema = Schema::default();
        schema.create_table("example").indexed_column("id", Type::NUMBER).column("content", Type::TEXT).add();
        let db = Database::with_schema(schema);

        let query = dsl::Query::scan_index("example.id", EQ, "1");
        let source = compute_plan(&db, &query).unwrap().source;
        {
            let attr = &source.attributes()[0];
            assert_eq!(attr.name(), "example.id");
            assert_eq!(attr.kind(), Type::NUMBER);
        }

        {
            let attr = &source.attributes()[1];
            assert_eq!(attr.name(), "example.content");
            assert_eq!(attr.kind(), Type::TEXT);
        }

        if let Source::IndexScan(_, val) = source {
            assert_eq!(val, Cell::from_i32(1));
        } else {
            panic!()
        }
    }

    fn source_attributes(source: &Source) -> Vec<Type> {
        source.attributes().iter().map(|attr| attr.kind()).collect()
    }

    fn attribute_names(attrs: &[Attribute]) -> Vec<&str> {
        attrs.iter().map(|attr| attr.name()).collect()
    }

    fn tuple(cells: &[(Type, &str)]) -> Vec<u8> {
        let mut tuple = Vec::new();
        for (kind, value) in cells.iter() {
            let cell = crate::tuple::Element{ name: "", raw: &bytes(*kind, value), kind: *kind };
            let mut serialized = cell.serialize();
            tuple.append(&mut serialized);
        }

        tuple
    }

    fn bytes(t: Type, s: &str) -> Vec<u8> {
        match t {
            Type::NUMBER => Vec::from(s.parse::<i32>().unwrap().to_be_bytes()),
            Type::TEXT => {
                let mut v = vec![s.len() as u8];
                s.as_bytes().iter().for_each(|c| v.push(*c));
                v
            },
            _ => panic!(),
        }
    }
}
