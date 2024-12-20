use crate::bytes::into_bytes;
use crate::database::SharedObject;
use crate::mapper::Mapper;
use crate::mapper::NoopMapper;
use crate::mapper::OutTuple;
use crate::mapper::SetMapper;
use crate::tuple::PositionalAttribute;
use crate::Database;
use crate::Operator;
use crate::dsl;
use crate::object::Attribute;
use crate::object::NamedAttribute as _;
use crate::schema::AttributeRef;
use crate::schema::Column;
use crate::schema::Type;
use crate::tuple::Tuple;

use core::fmt;
use std::collections::BTreeMap;
use std::collections::HashMap;
use std::rc::Rc;

type Result<T> = std::result::Result<T, String>;

#[derive(Default)]
pub(crate) struct Plan {
    pub source: Source,
    pub mappers: Vec<Box<dyn for<'a> Mapper<'a>>>,
    pub filters: Vec<Filter>,
    pub joins: Vec<Join>,
    pub finisher: Finisher,
}

impl Plan {
    pub fn final_attributes(&self) -> Vec<Attribute> {
        let mut attrs = Vec::new();

        attrs.extend(self.source.attributes());

        for join in &self.joins {
            attrs.extend(join.joiner.borrow().attributes().cloned());
        }

        attrs
    }

    pub(crate) fn is_result_immediate(&self) -> bool {
        match self.source {
            Source::Tuple(_) => true,
            _ => {
                match self.finisher {
                    Finisher::Return => false,
                    Finisher::Count => true,
                    Finisher::Apply(_, _) => true,
                    Finisher::Nil => true,
                }
            },
        }
    }
}

pub(crate) struct Filter {
    attribute: Attribute,
    right: Vec<u8>,
    comp: &'static dyn Fn(&[u8], &[u8]) -> bool,
}

impl Filter {
    pub fn matches_tuple<'a>(&self, tuple: &'a impl ByteTuple<'a>) -> bool {
        let left = tuple.bytes(&self.attribute).unwrap();
        (self.comp)(left, &self.right)
    }
}

pub(crate) trait ByteTuple<'a> {
    fn bytes(&'a self, attr: &impl PositionalAttribute) -> Option<&[u8]>;
}

impl<'a> ByteTuple<'a> for Tuple<'a> {
    fn bytes(&'a self, attr: &impl PositionalAttribute) -> Option<&[u8]> {
        self.raw_element(attr)
    }
}

impl<'a> ByteTuple<'a> for OutTuple<'a> {
    fn bytes(&self, attr: &impl PositionalAttribute) -> Option<&[u8]> {
        self.element(attr)
    }
}

pub(crate) struct Join {
    joiner: SharedObject,
    joinee_key: AttributeRef,
    joiner_key: AttributeRef,
}

impl Join {
    pub fn source_object(&self) -> &SharedObject {
        &self.joiner
    }

    pub fn joinee_key(&self) -> &AttributeRef {
        &self.joinee_key
    }

    pub fn joiner_key(&self) -> &AttributeRef {
        &self.joiner_key
    }
}

#[derive(Default)]
pub(crate) enum Source {
    TableScan(SharedObject),
    Tuple(BTreeMap<Attribute, String>),
    IndexScan(SharedObject, Vec<u8>),
    #[default]
    Nil,
}

impl Source {
    pub fn attributes(&self) -> Vec<Attribute> {
        match &self {
            Self::Tuple(values) => values.keys().cloned().collect(),
            Self::TableScan(obj) => obj.borrow().attributes().cloned().collect(),
            Self::IndexScan(obj, _) => obj.borrow().attributes().cloned().collect(),
            _ => vec![]
        }
    }
}

impl fmt::Display for Source {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::TableScan(o) => write!(f, "TableScan({})", o.borrow().id()),
            Self::Tuple(m) => write!(f, "Tuple({})", m.len()),
            Self::IndexScan(o, key) => write!(f, "IndexScan({}, {})", o.borrow().id(), key.len()),
            Self::Nil => write!(f, "Nil"),
        }
    }
}

enum QuerySource<'q> {
    Tuple(HashMap<&'q str, (Type, &'q str)>),
    Scan(&'q str),
    IndexScan(&'q str, &'q str),
}

impl<'q> QuerySource<'q> {
    fn into_source(self, db: &Database) -> Result<Source> {
        match self {
            Self::Tuple(values) => Ok(Source::from_map(&values)),
            Self::Scan(name) => {
                let obj = db.object(name).ok_or_else(|| format!("No such relation {name}"))?;
                Ok(Source::scan_table(obj))
            },
            Self::IndexScan(attr_name, val) => {
                let attr = db.schema().find_column(attr_name).ok_or_else(|| format!("No such attribute {attr_name}"))?;
                let obj = db.object(&attr.reference()).expect("Attribute found so the object must exist");
                if attr.indexed() {
                    let bytes = into_bytes(attr.kind(), val).map_err(|_| format!("Cannot parse {} as {}", val, attr.kind()))?;
                    Ok(Source::scan_index(obj, bytes))
                } else {
                    Err(format!("Attribute {attr_name} is not an index"))
                }
            },
        }
    }
}

enum QueryMapper<'q> {
    Set(&'q str, &'q str),
    Noop,
}

impl<'a> From<&'a dsl::Mapper<'a>> for QueryMapper<'a> {
    fn from(mapper: &'a dsl::Mapper<'a>) -> Self {
        match mapper.function {
            "set" => {
                match &mapper.args[..] {
                    [attr, value] => Self::Set(attr, value),
                    _ => panic!(),
                }
            },
            _ => Self::Noop,
        }
    }
}

fn compute_query_mappers<'q>(query: &'q dsl::Query) -> impl Iterator<Item = QueryMapper<'q>> {
    query.mappers.iter().map(From::from)
}

impl Source {
    fn scan_table(obj: &SharedObject) -> Self {
        Source::TableScan(Rc::clone(obj))
    }

    fn from_map(map: &HashMap<&str, (Type, &str)>) -> Self {
        let mut attributes = Vec::new();
        let mut values = BTreeMap::new();

        for (pos, (k, v)) in map.iter().enumerate() {
            let attr = Attribute { name: Box::from(*k), kind: v.0, reference: AttributeRef::temporary(pos as u32) };
            attributes.push(attr.clone());
            values.insert(attr, v.1.to_string());
        }

        Self::Tuple(values)
    }

    fn scan_index(obj: &SharedObject, val: Vec<u8>) -> Self {
        Self::IndexScan(Rc::clone(obj), val)
    }
}

#[derive(Default, Debug)]
pub(crate) enum Finisher {
    Return,
    Count,
    Apply(ApplyFn, AttributeRef),
    #[default]
    Nil,
}

impl Finisher {
    fn apply(name: &str, attr: Column) -> Self {
        Finisher::Apply(ApplyFn::from(name), attr.reference())
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

pub(crate) fn compute_plan(db: &Database, query: &dsl::Query) -> Result<Plan> {
    let source = compute_source(&query.source)?;
    let plan = compute_finisher(source, db, query)?;
    let plan = compute_joins(plan, db, query)?;
    let plan = compute_filters(plan, query)?;
    let plan = compute_mappers(plan, compute_query_mappers(query))?;

    Ok(plan)
}

fn compute_source<'query>(dsl_source: &'query dsl::Source) -> Result<QuerySource<'query>> {
    match dsl_source {
        dsl::Source::Tuple(values) => {
            let tuple = values.iter().map(|attr| {
                let name = attr.name.as_ref();
                let kind = attr.kind.unwrap_or(Type::NONE);
                let value = attr.value.as_ref();
                (name, (kind, value))
            }).collect();
            Ok(QuerySource::Tuple(tuple))
        },
        dsl::Source::TableScan(relation_name) => {
            Ok(QuerySource::Scan(relation_name))
        },
        dsl::Source::IndexScan(index, Operator::EQ, val) => {
            Ok(QuerySource::IndexScan(index, val))
        },
        _ => Err(String::from("Source not supported"))
    }
}

fn compute_filters(plan: Plan, query: &dsl::Query) -> Result<Plan> {
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
                right: right_as_bytes(attr.kind(), dsl_filter)?,
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

fn compute_mappers<'a>(mut plan: Plan, query_mappers: impl Iterator<Item = QueryMapper<'a>>) -> Result<Plan> {
    for query_mapper in query_mappers {
        match query_mapper {
            QueryMapper::Set(attr_name, value) => {
                let attributes = plan.final_attributes();
                let attr = attributes.iter().find(|attr| attr == attr_name).cloned().unwrap();
                plan.mappers.push(Box::new(SetMapper {
                    attributes_after: attributes.into(),
                    value: into_bytes(attr.kind(), value).unwrap().into(),
                    attr,
                }));
            },
            _ => {
                let attributes = plan.final_attributes();
                plan.mappers.push(Box::new(NoopMapper(attributes.into())));
            }
        }
    }

    Ok(plan)
}

fn compute_finisher<'query>(source: QuerySource<'query>, db: &Database, query: &'query dsl::Query) -> Result<Plan> {
    match &query.finisher {
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
        _ => unimplemented!(),
    }
}

fn right_as_bytes(kind: Type, dsl_filter: &dsl::Filter) -> Result<Vec<u8>> {
    let right = match dsl_filter {
        dsl::Filter::Condition(_, _, right) => right
    };

    into_bytes(kind, right).map_err(|e| format!("{e}"))
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

fn compute_joins(plan: Plan, db: &Database, query: &dsl::Query) -> Result<Plan> {
    if query.join_sources.is_empty() {
        return Ok(plan);
    }

    let mut joins = Vec::with_capacity(query.join_sources.len());
    let joinee = match &query.source {
        dsl::Source::TableScan(name) => Some(name),
        _ => todo!(),
    }.and_then(|name| db.schema().find_relation(*name)).ok_or("Relation {name} not found")?;

    for join_source in &query.join_sources {
        // TODO: Write a test for bi-directional join syntax
        let joinee_attr = joinee.lookup(join_source.left)
            .or_else(|| joinee.lookup(join_source.right))
            .ok_or_else(|| format!("Cannot find attribute {} or {}", join_source.left, join_source.right))?;

        let joiner_attr = db.schema().lookup_attribute(join_source.left)
            .filter(|a| !a.belongs_to(joinee))
            .or_else(|| db.schema().lookup_attribute(join_source.right))
            .filter(|a| !a.belongs_to(joinee))
            .ok_or_else(|| format!("Cannot find attribute {} or {}", join_source.left, join_source.right))?;

        joins.push(Join {
            joiner: Rc::clone(db.object(&joiner_attr).unwrap()),
            joinee_key: joinee_attr.reference(),
            joiner_key: joiner_attr.reference(),
        })
    }

    Ok(Plan{ joins, ..plan })
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use super::*;
    use crate::bytes::IntoBytes as _;
    use crate::dsl::TupleBuilder;
    use crate::dsl::Operator::{EQ, GT};
    use crate::object::TempObject;
    use crate::schema::{Schema, Type};
    use crate::Definition;


    #[test]
    fn test_compute_filter() {
        let mut db = Database::default();
        let command = Definition::relation("example")
            .attribute("id", Type::NUMBER)
            .attribute("content", Type::TEXT)
            .attribute("type", Type::NUMBER);
        db.define(&command).unwrap();

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
        db.define(&Definition::relation("example")
                          .attribute("id", Type::NUMBER)
                          .attribute("content", Type::TEXT)
                          .attribute("type", Type::NUMBER)).unwrap();

        let mut object = TempObject::from_relation(db.schema().find_relation("example").unwrap());
        object.push_str(&["1", "content1", "11"]);
        object.push_str(&["2", "content2", "12"]);
        let tuple_1 = object.iter().next().unwrap();
        let tuple_2 = object.iter().nth(1).unwrap();

        let id_filter = expect_filter(compute_plan(&db, &dsl::Query::scan("example").filter("example.id", EQ, "1")));
        assert!(id_filter.matches_tuple(&tuple_1));
        assert!(!id_filter.matches_tuple(&tuple_2));

        let content_filter = expect_filter(compute_plan(&db, &dsl::Query::scan("example").filter("example.content", EQ, "content1")));
        assert!(content_filter.matches_tuple(&tuple_1));
        assert!(!content_filter.matches_tuple(&tuple_2));

        let comp_filter = expect_filter(compute_plan(&db, &dsl::Query::scan("example").filter("example.id", GT, "1")));
        assert!(!comp_filter.matches_tuple(&tuple_1));
        assert!(comp_filter.matches_tuple(&tuple_2));
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
        db.define(&Definition::relation("document")
                          .attribute("name", Type::TEXT)
                          .attribute("type_id", Type::NUMBER)).unwrap();
        db.define(&Definition::relation("type")
                          .attribute("id", Type::NUMBER)
                          .attribute("name", Type::TEXT)).unwrap();

        let query = dsl::Query::scan("document").join("document.type_id", "type.id");
        let plan = compute_plan(&db, &query).unwrap();
        assert_eq!(attribute_names(&plan.final_attributes()), BTreeSet::from(["document.name", "document.type_id", "type.id", "type.name"]));

        let query = dsl::Query::scan("nothing").join("a", "b");
        let missing_source_table = compute_plan(&db, &query);
        assert!(missing_source_table.is_err());

        let query = dsl::Query::scan("document").join("a", "b");
        let missing_table = compute_plan(&db, &query);
        assert!(missing_table.is_err());

        let query = dsl::Query::scan("document").join("something", "else");
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
            .join("document.type_id", "type.id")
            .join("document.author", "author.username");
        let plan = compute_plan(&db, &query).unwrap();

        assert_eq!(
            attribute_names(&plan.final_attributes()),
            BTreeSet::from(["document.type_id", "document.author", "document.name", "type.id", "type.name", "author.username", "author.displayname"])
        );
    }

    #[test]
    fn test_filter_after_join() {
        let mut schema = Schema::default();
        schema.create_table("document").column("name", Type::TEXT).column("type_id", Type::NUMBER).add();
        schema.create_table("type").column("id", Type::NUMBER).column("name", Type::TEXT).add();
        let db = Database::with_schema(schema);

        let filter = expect_filter(compute_plan(&db, &dsl::Query::scan("document").join("document.type_id", "type.id").filter("type.name", EQ, "b")));
        assert_eq!(filter.attribute.name(), "type.name");
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
                .typed(Type::NUMBER, "id", "1")
                .typed(Type::TEXT, "name", "some text")
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
        assert_eq!(attribute_names(&result.final_attributes()), BTreeSet::from(["example.id", "example.content", "example.type"]));
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

        let query = dsl::Query::scan("example").count();
        let finisher = compute_plan(&db, &query)
            .unwrap().finisher;
        assert!(matches!(finisher, Finisher::Count));

        let query = dsl::Query::scan("example").apply("sum", &["example.id"]);
        let finisher = compute_plan(&db, &query)
            .unwrap().finisher;
        assert!(matches!(finisher, Finisher::Apply(ApplyFn::Sum, _)));
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
            assert_eq!(val, 1i32.to_byte_vec());
        } else {
            panic!()
        }
    }

    fn source_attributes(source: &Source) -> Vec<Type> {
        source.attributes().iter().map(|attr| attr.kind()).collect()
    }

    fn attribute_names(attrs: &[Attribute]) -> BTreeSet<&str> {
        attrs.iter().map(|attr| attr.name()).collect()
    }
}
