use crate::Cell;
use crate::Operator;
use crate::dsl;
use crate::schema::Column;
use crate::schema::{Schema, Relation, Type};
use crate::tuple::Tuple;

use std::collections::HashMap;
use std::iter::zip;

type Result<T> = std::result::Result<T, String>;    

#[derive(Default)]
pub(crate) struct Plan<'schema> {
    pub source: Source<'schema>,
    pub filters: Vec<Filter>,
    pub joins: Vec<Join<'schema>>,
    pub finisher: Finisher<'schema>,
}

impl<'schema> Plan<'schema> {
    #[cfg(test)]
    pub fn insert(rel: &'schema Relation, values: &[&str]) -> Self {
        let attributes = rel.attributes().enumerate()
            .map(|(pos, attr)| Attribute::named(pos, attr.name()).with_type(attr.kind())).collect();
        let contents = Contents::Tuple(values.iter().map(|s|String::from(*s)).collect());

        Self{
            source: Source{ attributes, contents },
            finisher: Finisher::Insert(rel),
            ..Plan::default()
        }
   }

    #[cfg(test)]
    pub fn scan(rel: &'schema Relation) -> Self {
        Self{
            source: Source::scan_table(rel),
            finisher: Finisher::Return,
            ..Plan::default()
        }
    }

    fn validate_with_finisher(self) -> Result<Self> {
        Ok(Plan{
            source: self.source.validate_with_finisher(&self.finisher)?,
            ..self
        })
    }

    pub fn final_attributes(&self) -> Vec<Attribute> {
        if let Some(last_join) = self.joins.iter().last() {
            last_join.attributes_after()
        } else {
            self.source.attributes.to_vec()
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
        let cell = tuple.cell_by_attr(&self.attribute);
        let left = cell.bytes();
        (self.comp)(left, &self.right)
    }
}

pub(crate) struct Join<'a> {
    table: &'a Relation,
    joinee_attributes: Vec<Attribute>,
    joiner_attributes: Vec<Attribute>,
    joinee_key: usize,
    joiner_key: usize,
}

impl<'a> Join<'a> {
    pub fn source_table(&self) -> &Relation {
        self.table
    }

    pub fn joinee_key(&self) -> &Attribute {
        self.joinee_attributes.get(self.joinee_key).unwrap()
    }

    pub fn joiner_key(&self) -> &Attribute {
        self.joiner_attributes.get(self.joiner_key).unwrap()
    }

    pub fn attributes_after(&self) -> Vec<Attribute> {
        self.joinee_attributes.iter()
            .chain(self.joiner_attributes.iter())
            .enumerate()
            .map(|(pos, attr)| Attribute { pos, name: Box::from(attr.name()), kind: attr.kind() })
            .collect()
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct Attribute {
    // TODO: position should be internal to the object (but not schema
    // because the element ordering should be private for the object)
    pub pos: usize,
    pub name: Box<str>,
    pub kind: Type,
}

#[derive(Default)]
pub(crate) struct Source<'a> {
    pub attributes: Vec<Attribute>,
    pub contents: Contents<'a>,
}

enum QuerySource<'q> {
    Tuple(HashMap<&'q str, (Type, &'q str)>),
    Scan(&'q str),
    IndexScan(&'q str, Cell),
}

impl<'q> QuerySource<'q> {
    fn into_source<'schema>(self, schema: &'schema Schema) -> Result<Source<'schema>> {
        match self {
            Self::Tuple(values) => Ok(Source::from_map(&values)),
            Self::Scan(name) => {
                let rel = schema.find_relation(name).ok_or_else(|| format!("No such relation {name}"))?;
                Ok(Source::scan_table(rel))
            },
            Self::IndexScan(attr_name, cell) => {
                let attr = schema.find_column(&attr_name).ok_or_else(|| format!("No such attribute {attr_name}"))?;
                if attr.indexed() {
                    Ok(Source::scan_index(attr, cell))
                } else {
                    Err(format!("Attribute {attr_name} is not an index"))
                }
            },
        }
    }
}

pub(crate) enum Contents<'a> {
    TableScan(&'a Relation),
    Tuple(Vec<String>),
    IndexScan(Column<'a>, Cell),
    Nil,
}

impl<'a> Default for Contents<'a> {
    fn default() -> Self {
        Self::Nil
    }
}

impl<'a> Contents<'a> {
    fn len(&self) -> usize {
        match self {
            Self::TableScan(rel) => rel.attributes().len(),
            Self::Tuple(vals) => vals.len(),
            Self::IndexScan(_, _) => 1,
            Self::Nil => 0,
        }
    }
}

impl Attribute {
    pub fn named(pos: usize, name: &str) -> Self {
        Self {
            pos,
            name: Box::from(name),
            kind: Type::default(),
        }
    }

    pub fn with_type(self, kind: Type) -> Self {
        Self {
            kind,
            ..self
        }
    }

    pub fn pos(&self) -> usize {
        self.pos
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn kind(&self) -> Type {
        self.kind
    }

    pub fn short_name(&self) -> &str {
        if let Some(i) = self.name.find('.') {
            &self.name[i+1..]
        } else {
            &self.name
        }
    }
}

impl PartialEq<str> for Attribute {
    fn eq(&self, s: &str) -> bool {
        self.name.as_ref() == s
    }
}

impl<'schema> Source<'schema> {
    fn scan_table(rel: &'schema Relation) -> Self {
        let attributes = rel.attributes().enumerate()
            .map(|(i, attr)| Attribute::named(i, attr.name()).with_type(attr.kind())).collect();
        Source{ attributes, contents: Contents::TableScan(rel) }
    }

    fn from_map(map: &HashMap<&str, (Type, &str)>) -> Self {
        let mut attributes = Vec::new();
        let mut values = Vec::new();

        for (pos, (k, v)) in map.into_iter().enumerate() {
            attributes.push(Attribute { pos, name: Box::from(*k), kind: v.0 });
            values.push(v.1.to_string());
        }

        Self { attributes, contents: Contents::Tuple(values) }
    }

    fn scan_index(index: Column<'schema>, val: Cell) -> Self {
        let attributes = index.table().attributes().enumerate()
            .map(|(i, attr)| Attribute::named(i, attr.name()).with_type(attr.kind())).collect();

        Self{
            attributes,
            contents: Contents::IndexScan(index, val),
        }
    }

    fn validate_with_finisher(self, finisher: &Finisher) -> Result<Source<'schema>> {
        match finisher {
            Finisher::Insert(rel) => {
                if let Contents::Tuple(values) = &self.contents {
                    let target_types = rel.types();
                    if target_types.len() != self.attributes.len() || target_types.len() != self.contents.len() {
                        return Err(format!("Cannot insert tuple with {} attributes into relation with {} attributes", self.attributes.len(), target_types.len()));

                    }

                    if zip(values.iter(), target_types.iter()).any(|(val, expected)| !validate_type(val, *expected)) {
                        return Err(String::from("Type incompatible with the target table"));
                    }

                    Ok(self)
                } else {
                    Err(String::from("Expected a tuple source for insert"))
                }
            },
            _ => {
                Ok(Source{ attributes: self.attributes, contents: self.contents })
            },
        }
    }
}

#[derive(Default, PartialEq, Debug)]
pub enum Finisher<'a> {
    Return,
    Insert(&'a Relation),
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

pub(crate) fn compute_plan<'schema>(schema: &'schema Schema, query: &dsl::Query) -> Result<Plan<'schema>> {
    let source = compute_source(&query.source)?;
    let plan = compute_finisher(source, schema, query)?;
    let plan = plan.validate_with_finisher()?;

    let plan = compute_joins(plan, schema, query)?;

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

fn compute_finisher<'query, 'schema>(source: QuerySource<'query>, schema: &'schema Schema, query: &'query dsl::Query) -> Result<Plan<'schema>> {
    match &query.finisher {
        dsl::Finisher::Insert(name) => {
            if let QuerySource::Tuple(map) = source {
                let relation = schema.find_relation(*name).ok_or("No such target table")?;
                let finisher = Finisher::Insert(relation);
                let attributes: Vec<_> = relation.attributes().enumerate()
                    .map(|(pos, attr)| Attribute::named(pos, attr.name()).with_type(attr.kind())).collect();
                let mut values = Vec::new();
                for attr in &attributes {
                    if let Some((_, val)) = map.get(attr.name.as_ref()) {
                        values.push(val.to_string());
                    } else if let Some((_, val)) = map.get(attr.short_name()) {
                        values.push(val.to_string());
                    } else {
                        return Err(format!("Missing attribute in source {}", attr.name));
                    }
                }
                let source = Source{ attributes, contents: Contents::Tuple(values) };
                Ok(Plan{ source, finisher, ..Default::default() })
            } else {
                panic!()
            }
        },
        dsl::Finisher::AllColumns => Ok(Plan{ finisher: Finisher::Return, source: source.into_source(&schema)?, ..Default::default() }),
        dsl::Finisher::Count => Ok(Plan{ finisher: Finisher::Count, source: source.into_source(&schema)?, ..Default::default() }),
        dsl::Finisher::Apply(f, args) => {
            let finisher = args
                .first()
                .and_then(|n| schema.find_column(n))
                .map(|attr| Ok(Finisher::apply(f, attr)))
                .unwrap_or(Err("apply: No such attribute"))?;
            Ok(Plan{ finisher, source: source.into_source(&schema)?, ..Default::default() })
        },
        dsl::Finisher::Delete => {
            let finisher = match &query.source {
                dsl::Source::TableScan(name) => schema.find_relation(*name).map(Finisher::Delete).ok_or("No such relation found for delete operation"),
                _ => Err("Illegal delete operation"),
            }?;
            Ok(Plan{ finisher, source: source.into_source(&schema)?, ..Default::default() })
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

        let joinee_attributes = table_attributes(joinee_table);
        let joiner_attributes = table_attributes(joiner_table);
        if let Some(joinee_key) = joinee_attributes.iter().enumerate().find(|(_, a)| a == &join_source.left).map(|(i,_)| i) {
            if let Some(joiner_key) = joiner_attributes.iter().enumerate().find(|(_, a)| a == &join_source.right).map(|(i,_)| i) {
                joins.push(Join{
                    table: joiner_table,
                    joinee_attributes, joiner_attributes,
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

fn table_attributes(table: &Relation) -> Vec<Attribute> {
    table.attributes()
        .map(|col| Attribute { pos: col.pos(), name: Box::from(col.name()), kind: col.kind() })
        .collect()
}

fn validate_type(value: &str, kind: Type) -> bool {
    match kind {
        Type::NUMBER => value.parse::<i32>().is_ok(),
        Type::TEXT => true,
        Type::BOOLEAN => value.parse::<bool>().is_ok(),
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dsl::{AttrKind, TupleBuilder};
    use crate::dsl::Operator::{EQ, GT};
    use crate::schema::Type;


    #[test]
    fn test_compute_filter() {
        let mut schema = Schema::default();
        schema.create_table("example").column("id", Type::NUMBER).column("content", Type::TEXT).column("type", Type::NUMBER).add();

        let filters = expect_filters(compute_plan(&schema, &dsl::Query::scan("example").filter("example.id", EQ, "1").filter("example.type", EQ, "2")), 2);
        assert_eq!(filters.get(0).map(|x| x.attribute.pos()), Some(0));
        assert_eq!(filters.get(1).map(|x| x.attribute.pos()), Some(2));

        let query = dsl::Query::scan("example").filter("not-a-column", EQ, "0");
        let failure = compute_plan(&schema, &query);
        assert!(failure.is_err());
    }

    #[test]
    fn test_apply_filter() {
        let mut schema = Schema::default();
        schema.create_table("example").column("id", Type::NUMBER).column("content", Type::TEXT).column("type", Type::NUMBER).add();
        let attributes: Vec<Attribute> = schema.find_relation("example").unwrap().attributes().map(Into::into).collect();
        let tuple_1 = tuple(&[(Type::NUMBER, "1"), (Type::TEXT, "content1"), (Type::NUMBER, "11")]);
        let tuple_2 = tuple(&[(Type::NUMBER, "2"), (Type::TEXT, "content2"), (Type::NUMBER, "12")]);

        let id_filter = expect_filter(compute_plan(&schema, &dsl::Query::scan("example").filter("example.id", EQ, "1")));
        assert!(id_filter.matches_tuple(&Tuple::from_bytes(&tuple_1, &attributes)));
        assert!(!id_filter.matches_tuple(&Tuple::from_bytes(&tuple_2, &attributes)));

        let content_filter = expect_filter(compute_plan(&schema, &dsl::Query::scan("example").filter("example.content", EQ, "content1")));
        assert!(content_filter.matches_tuple(&Tuple::from_bytes(&tuple_1, &attributes)));
        assert!(!content_filter.matches_tuple(&Tuple::from_bytes(&tuple_2, &attributes)));

        let comp_filter = expect_filter(compute_plan(&schema, &dsl::Query::scan("example").filter("example.id", GT, "1")));
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
        let mut schema = Schema::default();
        schema.create_table("document").column("name", Type::TEXT).column("type_id", Type::NUMBER).add();
        schema.create_table("type").column("id", Type::TEXT).column("name", Type::NUMBER).add();

        let query = dsl::Query::scan("document").join("type", "document.type_id", "type.id");
        let joins = expect_join(compute_plan(&schema, &query));
        assert_eq!(joins.source_table().name.as_ref(), "type");
        assert_eq!(attribute_names(&joins.attributes_after()), vec!["document.name", "document.type_id", "type.id", "type.name"]);

        let query = dsl::Query::scan("nothing").join("type", "a", "b");
        let missing_source_table = compute_plan(&schema, &query);
        assert!(missing_source_table.is_err());

        let query = dsl::Query::scan("document").join("nothing", "a", "b");
        let missing_table = compute_plan(&schema, &query);
        assert!(missing_table.is_err());

        let query = dsl::Query::scan("document").join("type", "something", "else");
        let missing_column = compute_plan(&schema, &query);
        assert!(missing_column.is_err());
    }

    #[test]
    fn test_plan_multiple_joins() {
        let mut schema = Schema::default();
        schema.create_table("document").column("name", Type::TEXT).column("type_id", Type::NUMBER).column("author", Type::TEXT).add();
        schema.create_table("type").column("id", Type::TEXT).column("name", Type::NUMBER).add();
        schema.create_table("author").column("username", Type::TEXT).column("displayname", Type::TEXT).add();

        let query = dsl::Query::scan("document")
            .join("type", "document.type_id", "type.id")
            .join("author", "document.author", "author.username");
        let joins = expect_joins(compute_plan(&schema, &query), 2);
        assert_eq!(joins[0].table.name.as_ref(), "type");
        assert_eq!(joins[1].table.name.as_ref(), "author");
    }

    #[test]
    fn test_filter_after_join() {
        let mut schema = Schema::default();
        schema.create_table("document").column("name", Type::TEXT).column("type_id", Type::NUMBER).add();
        schema.create_table("type").column("id", Type::NUMBER).column("name", Type::TEXT).add();

        let filter = expect_filter(compute_plan(&schema, &dsl::Query::scan("document").join("type", "document.type_id", "type.id").filter("type.name", EQ, "b")));
        assert_eq!(filter.attribute.pos(), 3);
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
        let schema = Schema::default();
        let query = dsl::Query::tuple(&[("id", "1"), ("value", "some text")]).filter("id", Operator::EQ, "1");
        let result = compute_plan(&schema, &query);
        assert!(result.is_err());
        assert_eq!(result.err(), Some(String::from("Cannot filter untyped attribute id")));

        let query = dsl::Query::tuple(
            TupleBuilder::new()
                .typed(AttrKind::Number, "id", "1")
                .typed(AttrKind::Text, "name", "some text")
        ).filter("id", Operator::EQ, "1");
        let result = compute_plan(&schema, &query);

        let filter = expect_filter(result);
        assert_eq!(filter.attribute.name.as_ref(), "id");
    }

    #[test]
    fn test_source_types_for_select() {
        let mut schema = Schema::default();
        schema.create_table("example").column("id", Type::NUMBER).column("content", Type::TEXT).column("type", Type::NUMBER).add();

        let query = dsl::Query::scan("example");
        let result = compute_plan(&schema, &query).unwrap();
        assert_eq!(source_attributes(&result.source), vec![Type::NUMBER, Type::TEXT, Type::NUMBER]);
        assert_eq!(attribute_names(&result.final_attributes()), vec!["example.id", "example.content", "example.type"]);
    }

    #[test]
    fn test_source_types_for_insert() {
        let mut schema = Schema::default();
        schema.create_table("example").column("id", Type::NUMBER).column("content", Type::TEXT).column("title", Type::TEXT).add();

        let query = dsl::Query::tuple(&[("id", "1"), ("content", "the-content"), ("title", "title")]).insert_into("example");
        let result = compute_plan(&schema, &query).unwrap();
        assert_eq!(source_attributes(&result.source), vec![Type::NUMBER, Type::TEXT, Type::TEXT]);
        assert_eq!(attribute_names(&result.final_attributes()), vec!["example.id", "example.content", "example.title"]);

        let query = dsl::Query::tuple(&[("id", "1"), ("content", "the-content")]).insert_into("example");
        let wrong_tuple_len = compute_plan(&schema, &query);
        assert!(wrong_tuple_len.is_err());

        let query = dsl::Query::tuple(&[("id", "not-ID"), ("content", "the-content"), ("title", "something")]).insert_into("example");
        let wrong_type = compute_plan(&schema, &query);
        assert!(wrong_type.is_err());
    }

    #[test]
    fn test_compute_finisher() {
        let mut schema = Schema::default();
        schema.create_table("example").column("id", Type::NUMBER).column("content", Type::TEXT).add();

        let query = dsl::Query::scan("example");
        let finisher = compute_plan(&schema, &query)
            .unwrap().finisher;
        assert_eq!(finisher, Finisher::Return);

        let query = dsl::Query::tuple(&[("id", "1"), ("content", "the content")]).insert_into("example");
        let finisher = compute_plan(&schema, &query)
            .unwrap().finisher;
        assert!(matches!(finisher, Finisher::Insert{..}));

        let query = dsl::Query::scan("example").count();
        let finisher = compute_plan(&schema, &query)
            .unwrap().finisher;
        assert_eq!(finisher, Finisher::Count);

        let query = dsl::Query::scan("example").apply("sum", &["example.id"]);
        let finisher = compute_plan(&schema, &query)
            .unwrap().finisher;
        assert!(matches!(finisher, Finisher::Apply(ApplyFn::Sum, _)));

        let query = dsl::Query::tuple(&[("id", "1")]).insert_into("not-a-table");
        let failure = compute_plan(&schema, &query);
        assert!(failure.is_err());
    }

    #[test]
    fn test_match_types_for_insert() {
        let mut schema = Schema::default();
        schema.create_table("example").column("id", Type::NUMBER).column("content", Type::TEXT).add();

        let query = dsl::Query::tuple(&[("id", "1"), ("content", "aaa")]).insert_into("example");
        let plan = compute_plan(&schema, &query).unwrap();

        assert_eq!(plan.source.attributes[0], Attribute::named(0, "example.id").with_type(Type::NUMBER));
        assert_eq!(plan.source.attributes[1], Attribute::named(1, "example.content").with_type(Type::TEXT));
    }

    #[test]
    fn test_scan_index() {
        let mut schema = Schema::default();
        schema.create_table("example").indexed_column("id", Type::NUMBER).column("content", Type::TEXT).add();

        let query = dsl::Query::scan_index("example.id", EQ, "1");
        let source = compute_plan(&schema, &query).unwrap().source;
        assert_eq!(source.attributes[0], Attribute::named(0, "example.id").with_type(Type::NUMBER));
        assert_eq!(source.attributes[1], Attribute::named(1, "example.content").with_type(Type::TEXT));
        if let Contents::IndexScan(_, val) = source.contents {
            assert_eq!(val, Cell::from_i32(1));
        } else {
            panic!()
        }
    }

    fn source_attributes(source: &Source) -> Vec<Type> {
        source.attributes.iter().map(|attr| attr.kind()).collect()
    }

    fn attribute_names(attrs: &[Attribute]) -> Vec<&str> {
        attrs.iter().map(|attr| attr.name()).collect()
    }

    fn tuple(cells: &[(Type, &str)]) -> Vec<u8> {
        let mut tuple = Vec::new();
        for (kind, value) in cells.iter() {
            let cell = crate::tuple::Cell{ name: "", raw: &bytes(*kind, value), kind: *kind };
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
