use crate::Cell;
use crate::Operator;
use crate::dsl;
use crate::schema::Column;
use crate::schema::{Schema, Relation, Type};
use crate::tuple::Tuple;

use std::iter::zip;

#[derive(Default)]
pub(crate) struct Plan<'a> {
    pub source: Source<'a>,
    pub filters: Vec<Filter>,
    pub joins: Vec<Join<'a>>,
    pub finisher: Finisher<'a>,
}

impl<'a> Plan<'a> {
    #[cfg(test)]
    pub fn insert(rel: &'a Relation, values: &[String]) -> Self {
        Self{
            source: Source::from_tuple(values.to_vec()),
            finisher: Finisher::Insert(rel),
            ..Plan::default()
        }
    }

    #[cfg(test)]
    pub fn scan(rel: &'a Relation) -> Self {
        Self{
            source: Source::scan_table(rel),
            finisher: Finisher::Return,
            ..Plan::default()
        }
    }

    fn validate_with_finisher(self) -> Result<Self, &'static str> {
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
            .map(|(pos, attr)| Attribute::Named(pos, attr.name(), attr.kind()))
            .collect()
    }
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) enum Attribute {
    Numbered(usize, Type),
    Named(usize, String, Type),
}

#[derive(Default)]
pub(crate) struct Source<'a> {
    pub attributes: Vec<Attribute>,
    pub contents: Contents<'a>,
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

impl Attribute {
    pub fn numbered(num: usize) -> Attribute {
        Attribute::Numbered(num, Type::default())
    }

    pub fn named(pos: usize, name: &str) -> Attribute {
        Attribute::Named(pos, name.to_string(), Type::default() )
    }

    pub fn with_type(self, kind: Type) -> Attribute {
        match self {
            Self::Numbered(pos, _) => Self::Numbered(pos, kind),
            Self::Named(pos, name, _) => Self::Named(pos, name, kind),
        }
    }

    fn guess_type(self, value: &str) -> Attribute {
        let kind = if value.parse::<i32>().is_ok() {
            Type::NUMBER
        } else {
            Type::TEXT
        };

        self.with_type(kind)
    }

    pub fn pos(&self) -> usize {
        match self {
            Self::Numbered(i, _) => *i,
            Self::Named(i, _, _) => *i,
        }
    }

    pub fn into_name(self) -> String {
        match self {
            Self::Numbered(i, _) => i.to_string(),
            Self::Named(_, s, _) => s,
        }
    }

    fn name(&self) -> String {
        match self {
            Self::Numbered(i, _) => i.to_string(),
            Self::Named(_, s, _) => s.to_string(),
        }
    }

    pub fn kind(&self) -> Type {
        match self {
            Self::Numbered(_, t) => *t,
            Self::Named(_, _, t) => *t,
        }
    }
}

impl PartialEq<str> for Attribute {
    fn eq(&self, s: &str) -> bool {
        match self {
            Self::Numbered(i, _) => s.parse::<usize>().map_or(false, |n| n == *i),
            Self::Named(_, name, _) => name == s,
        }
    }
}

impl<'a> Source<'a> {
    fn new(schema: &'a Schema, dsl_source: &dsl::Source) -> Result<Source<'a>, &'static str> {
        match dsl_source {
            dsl::Source::TableScan(name) => {
                Ok(Self::scan_table(schema.find_relation(*name).ok_or("No such table")?))
            },

            dsl::Source::Tuple(values) => {
                Ok(Self::from_tuple(values.iter().map(|s| s.to_string()).collect()))
            },
            dsl::Source::IndexScan(index, Operator::EQ, val) => {
                let index = schema.find_column(index)
                    .and_then(|col| if col.indexed() { Some(col) } else { None })
                    .ok_or("Index not found")?;
                let val = Cell::from_string(val);
                Ok(Self::scan_index(index, val))
            }
            _ => Err("Not supported source"),
        }
    }

    fn scan_table(rel: &'a Relation) -> Self {
        let attributes = rel.attributes().iter().enumerate()
            .map(|(i, (name, kind))| Attribute::named(i, name).with_type(*kind)).collect();
        Source{ attributes, contents: Contents::TableScan(rel) }
    }

    fn from_tuple(values: Vec<String>) -> Self {
        let attributes = values.iter().enumerate().map(|(i, val)| Attribute::numbered(i).guess_type(val)).collect();
        Self{ attributes, contents: Contents::Tuple(values.to_vec()) }
    }

    fn scan_index(index: Column<'a>, val: Cell) -> Self {
        let attributes = index.table().attributes().iter().enumerate()
            .map(|(i, (name, kind))| Attribute::named(i, name).with_type(*kind)).collect();
        Self{
            attributes,
            contents: Contents::IndexScan(index, val),
        }
    }

    fn validate_with_finisher(self, finisher: &Finisher) -> Result<Source<'a>, &'static str> {
        match finisher {
            Finisher::Insert(rel) => {
                let target_types = rel.types();
                if target_types.len() != self.attributes.len() {
                    return Err("Number of attributes in the tuple doesn't match the target table");
                }

                if zip(&self.attributes, target_types).any(|(attr, expected)| attr.kind() != expected) {
                        return Err("Type incompatible with the target table");
                }

                Ok(Source{ attributes: self.attributes, contents: self.contents })
            }

            _ => {
                Ok(Source{ attributes: self.attributes, contents: self.contents })
            },
        }
    }
}

#[derive(Debug, Default, PartialEq)]
pub enum Finisher<'a> {
    Return,
    Insert(&'a Relation),
    Count,
    Delete(&'a Relation),
    #[default]
    Nil,
}

pub(crate) fn compute_plan<'a>(schema: &'a Schema, query: &dsl::Query) -> Result<Plan<'a>, &'static str> {
    let plan: Plan = compute_source(schema, &query.source)?;

    let plan = compute_finisher(plan, schema, query)?;
    let plan = plan.validate_with_finisher()?;

    let plan = compute_joins(plan, schema, query)?;

    let plan = compute_filters(plan, query)?;

    Ok(plan)
}

fn compute_source<'a>(schema: &'a Schema, dsl_source: &dsl::Source) -> Result<Plan<'a>, &'static str> {
    Ok(Plan{
        source: Source::new(schema, dsl_source)?,
        ..Plan::default()
    })
}

fn compute_filters<'a>(plan: Plan<'a>, query: &dsl::Query) -> Result<Plan<'a>, &'static str> {
    if query.filters.is_empty() {
        return Ok(plan);
    }

    let attributes = plan.final_attributes();
    let mut filters = Vec::with_capacity(query.filters.len());
    for dsl_filter in &query.filters {
        if let Some(attr) = attributes.iter().find(|attr| attr == &filter_left(dsl_filter)) {
            let op = filter_operator(dsl_filter);
            filters.push(Filter{
                attribute: attr.clone(),
                right: right_as_cell(dsl_filter),
                comp: match op {
                    dsl::Operator::EQ => &|a, b| a == b,
                    dsl::Operator::GT => &|a, b| a > b,
                    dsl::Operator::GE => &|a, b| a >= b,
                    dsl::Operator::LT => &|a, b| a < b,
                    dsl::Operator::LE => &|a, b| a <= b,
                }
            });
        } else {
            return Err("Column not found");
        }
    }

    Ok(Plan{ filters, ..plan })
}

fn compute_finisher<'a>(plan: Plan<'a>, schema: &'a Schema, query: &dsl::Query) -> Result<Plan<'a>, &'static str> {
    match &query.finisher {
        dsl::Finisher::Insert(name) => schema.find_relation(*name).map(Finisher::Insert).ok_or("No such target table"),
        dsl::Finisher::AllColumns => Ok(Finisher::Return),
        dsl::Finisher::Count => Ok(Finisher::Count),
        dsl::Finisher::Delete => {
            match &query.source {
                dsl::Source::TableScan(name) => schema.find_relation(*name).map(Finisher::Delete).ok_or("No table to delete"),
                _ => Err("Illegal delete operation"),
            }
        }
        _ => todo!(),
    }.map(|finisher| Plan{ finisher, ..plan })
}

fn right_as_cell(dsl_filter: &dsl::Filter) -> Vec<u8> {
    let right = match dsl_filter {
        dsl::Filter::Condition(_, _, right) => right
    };
    
    Cell::from_string(right).into_bytes()

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

fn compute_joins<'a>(plan: Plan<'a>, schema: &'a Schema, query: &dsl::Query) -> Result<Plan<'a>, &'static str> {
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
            None => return Err("No such table")
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
                return Err("Joiner: no such column")
            }
        } else {
            return Err("Joinee: no such column")
        }
    }

    Ok(Plan{ joins, ..plan })
}

fn table_attributes(table: &Relation) -> Vec<Attribute> {
    table.columns().peekable()
        .map(|col| Attribute::Named(col.pos(), col.as_full_name(), col.kind()))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dsl::Operator::{EQ, GT};
    use crate::schema::Type;
    

    type ByteTuple = Vec<Vec<u8>>;

    #[test]
    fn test_compute_filter() {
        let mut schema = Schema::default();
        schema.create_table("example").column("id", Type::NUMBER).column("content", Type::TEXT).column("type", Type::NUMBER).add();

        let filters = expect_filters(compute_plan(&schema, &dsl::Query::scan("example").filter("example.id", EQ, "1").filter("example.type", EQ, "2")), 2);
        assert_eq!(filters.get(0).map(|x| x.attribute.pos()), Some(0));
        assert_eq!(filters.get(1).map(|x| x.attribute.pos()), Some(2));

        let failure = compute_plan(&schema, &dsl::Query::scan("example").filter("not-a-column", EQ, "0"));
        assert!(failure.is_err());
    }

    #[test]
    fn test_apply_filter() {
        let mut schema = Schema::default();
        schema.create_table("example").column("id", Type::NUMBER).column("content", Type::TEXT).column("type", Type::NUMBER).add();
        let tuple_1 = tuple(&["1", "content1", "11"]);
        let tuple_2 = tuple(&["2", "content2", "12"]);

        let id_filter = expect_filter(compute_plan(&schema, &dsl::Query::scan("example").filter("example.id", EQ, "1")));
        assert!(id_filter.matches_tuple(&Tuple::from_bytes(&tuple_1)));
        assert!(!id_filter.matches_tuple(&Tuple::from_bytes(&tuple_2)));

        let content_filter = expect_filter(compute_plan(&schema, &dsl::Query::scan("example").filter("example.content", EQ, "content1")));
        assert!(content_filter.matches_tuple(&Tuple::from_bytes(&tuple_1)));
        assert!(!content_filter.matches_tuple(&Tuple::from_bytes(&tuple_2)));

        let comp_filter = expect_filter(compute_plan(&schema, &dsl::Query::scan("example").filter("example.id", GT, "1")));
        assert!(!comp_filter.matches_tuple(&Tuple::from_bytes(&tuple_1)));
        assert!(comp_filter.matches_tuple(&Tuple::from_bytes(&tuple_2)));
    }

    fn expect_filters(result: Result<Plan, &'static str>, n: usize) -> Vec<Filter> {
        match result {
            Ok(plan) => {
                assert_eq!(plan.filters.len(), n);
                plan.filters
            },
            Err(msg) => panic!("{}", msg)
        }
    }

    fn expect_filter(result: Result<Plan, &'static str>) -> Filter {
        let filters = expect_filters(result, 1);
        filters.into_iter().next().unwrap()
    }

    #[test]
    fn test_compute_join() {
        let mut schema = Schema::default();
        schema.create_table("document").column("name", Type::TEXT).column("type_id", Type::NUMBER).add();
        schema.create_table("type").column("id", Type::TEXT).column("name", Type::NUMBER).add();

        let joins = expect_join(compute_plan(&schema, &dsl::Query::scan("document").join("type", "document.type_id", "type.id")));
        assert_eq!(joins.source_table().name, "type");
        assert_eq!(attribute_names(&joins.attributes_after()), vec!["document.name", "document.type_id", "type.id", "type.name"]);

        let missing_source_table = compute_plan(&schema, &dsl::Query::scan("nothing").join("type", "a", "b"));
        assert!(missing_source_table.is_err());

        let missing_table = compute_plan(&schema, &dsl::Query::scan("document").join("nothing", "a", "b"));
        assert!(missing_table.is_err());

        let missing_column = compute_plan(&schema, &dsl::Query::scan("document").join("type", "something", "else"));
        assert!(missing_column.is_err());
    }

    #[test]
    fn test_filter_after_join() {
        let mut schema = Schema::default();
        schema.create_table("document").column("name", Type::TEXT).column("type_id", Type::NUMBER).add();
        schema.create_table("type",).column("id", Type::TEXT).column("name", Type::NUMBER).add();

        let filter = expect_filter(compute_plan(&schema, &dsl::Query::scan("document").join("type", "document.type_id", "type.id").filter("type.name", EQ, "b")));
        assert_eq!(filter.attribute.pos(), 3);
    }

    fn expect_join<'a>(result: Result<Plan<'a>, &str>) -> Join<'a> {
        let plan = result.unwrap();
        assert_eq!(plan.joins.len(), 1);
        plan.joins.into_iter().next().unwrap()
    }

    #[test]
    fn test_source_types_for_select() {
        let mut schema = Schema::default();
        schema.create_table("example").column("id", Type::NUMBER).column("content", Type::TEXT).column("type", Type::NUMBER).add();

        let result = compute_plan(&schema, &dsl::Query::scan("example")).unwrap();
        assert_eq!(source_attributes(&result.source), vec![Type::NUMBER, Type::TEXT, Type::NUMBER]);
        assert_eq!(attribute_names(&result.final_attributes()), vec!["example.id", "example.content", "example.type"]);
    }

    #[test]
    fn test_source_types_for_insert() {
        let mut schema = Schema::default();
        schema.create_table("example").column("id", Type::NUMBER).column("content", Type::TEXT).column("title", Type::TEXT).add();

        let result = compute_plan(&schema, &dsl::Query::tuple(&["1", "the-content", "title"]).insert_into("example")).unwrap();
        assert_eq!(source_attributes(&result.source), vec![Type::NUMBER, Type::TEXT, Type::TEXT]);
        assert_eq!(attribute_names(&result.final_attributes()), vec!["0", "1", "2"]);

        let wrong_tuple_len = compute_plan(&schema, &dsl::Query::tuple(&["1", "the-content"]).insert_into("example"));
        assert!(wrong_tuple_len.is_err());

        let wrong_type = compute_plan(&schema, &dsl::Query::tuple(&["not-ID", "the-content", "something"]).insert_into("example"));
        assert!(wrong_type.is_err());
    }

    #[test]
    fn test_compute_finisher() {
        let mut schema = Schema::default();
        schema.create_table("example").column("id", Type::NUMBER).column("content", Type::TEXT).add();

        let finisher = compute_plan(&schema, &dsl::Query::scan("example"))
            .unwrap().finisher;
        assert_eq!(finisher, Finisher::Return);

        let finisher = compute_plan(&schema, &dsl::Query::tuple(&["1", "the content"]).insert_into("example"))
            .unwrap().finisher;
        assert!(matches!(finisher, Finisher::Insert{..}));

        let finisher = compute_plan(&schema, &dsl::Query::scan("example").count())
            .unwrap().finisher;
        assert_eq!(finisher, Finisher::Count);


        let failure = compute_plan(&schema, &dsl::Query::tuple(&["a", "b", "c"]).insert_into("not-a-table"));
        assert!(failure.is_err());
    }

    #[test]
    fn test_scan_index() {
        let mut schema = Schema::default();
        schema.create_table("example").indexed_column("id", Type::NUMBER).column("content", Type::TEXT).add();

        let source = compute_plan(&schema, &dsl::Query::scan_index("example.id", EQ, "1")).unwrap().source;
        assert_eq!(source.attributes[0], Attribute::Named(0, "example.id".to_string(), Type::NUMBER));
        assert_eq!(source.attributes[1], Attribute::Named(1, "example.content".to_string(), Type::TEXT));
        if let Contents::IndexScan(_, val) = source.contents {
            assert_eq!(val, Cell::from_number(1));
        } else {
            panic!()
        }
    }

    fn source_attributes(source: &Source) -> Vec<Type> {
        source.attributes.iter().map(|attr| attr.kind()).collect()
    }

    fn attribute_names(attrs: &[Attribute]) -> Vec<String> {
        attrs.iter().map(|attr| attr.name()).collect()
    }

    fn tuple(cells: &[&str]) -> ByteTuple {
        cells.iter().map(|s| cell(s)).collect()
    }

    fn cell(s: &str) -> Vec<u8> {
        if let Result::Ok(num) = s.parse::<i32>() {
            Vec::from(num.to_be_bytes())
        } else {
            Vec::from(s.as_bytes())
        }
    }
}

