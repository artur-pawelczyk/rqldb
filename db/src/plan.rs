use crate::Cell;
use crate::dsl;
use crate::schema::{Schema, Relation, Type};
use crate::db::Tuple;

use std::iter::zip;

#[derive(Default)]
pub(crate) struct Plan<'a> {
    pub source: Source<'a>,
    pub filters: Vec<Filter>,
    pub joins: Vec<Join>,
    pub finisher: Finisher<'a>,
}

impl<'a> Plan<'a> {
    fn validate_with_finisher(self) -> Result<Self, &'static str> {
        Ok(Plan{
            source: self.source.validate_with_finisher(&self.finisher)?,
            ..self
        })
    }

    pub fn source_attributes(&self) -> Vec<Attribute> {
        self.source.attributes.to_vec()
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
    right: Cell,
    comp: Box<dyn Fn(&Cell, &Cell) -> bool>,
}

impl Filter {
    pub fn matches_tuple(&self, tuple: &impl Tuple) -> bool {
        let left = tuple.cell(&self.attribute);
        (self.comp)(&left, &self.right)
    }
}

pub(crate) struct Join {
    table: String,
    pub joinee_attributes: Vec<Attribute>,
    pub joiner_attributes: Vec<Attribute>,
    joinee_key: usize,
    joiner_key: usize,
}

impl Join {
    pub fn find_match<'a, T: Tuple>(&self, joiner_tuples: &'a [T], joinee: &impl Tuple) -> Option<&'a T> {
        let joinee_key = joinee.cell(&self.joinee_key());

        joiner_tuples.iter()
            .find(|tuple| tuple.cell(&self.joiner_key()) == joinee_key)
    }

    pub fn source_table(&self) -> &str {
        &self.table
    }

    fn joinee_key(&self) -> &Attribute {
        self.joinee_attributes.get(self.joinee_key).unwrap()
    }

    fn joiner_key(&self) -> &Attribute {
        self.joiner_attributes.get(self.joiner_key).unwrap()
    }

    fn attributes_after(&self) -> Vec<Attribute> {
        self.joinee_attributes.iter()
            .chain(self.joiner_attributes.iter())
            .enumerate()
            .map(|(pos, attr)| Attribute{ pos, name: attr.name.to_string(), kind: attr.kind })
            .collect()
    }
}

#[derive(Clone, Debug)]
pub(crate) struct Attribute {
    pub pos: usize,
    pub name: String,
    pub kind: Type,
}

#[derive(Default)]
pub(crate) struct Source<'a> {
    pub attributes: Vec<Attribute>,
    pub contents: Contents<'a>,
}

pub(crate) enum Contents<'a> {
    TableScan(&'a Relation),
    Tuple(Vec<String>),
    Nil,
}

impl<'a> Default for Contents<'a> {
    fn default() -> Self {
        Self::Nil
    }
}

impl Attribute {
    pub fn numbered(num: usize) -> Attribute {
        Attribute{ pos: num, name: num.to_string(), kind: Type::default() }
    }

    pub fn named(pos: usize, name: &str) -> Attribute {
        Attribute{ pos, name: name.to_string(), kind: Type::default() }
    }

    pub fn with_type(self, kind: Type) -> Attribute {
        Attribute{ pos: self.pos, name: self.name, kind }
    }

    fn guess_type(self, value: &str) -> Attribute {
        let kind = Cell::from_string(value).kind; // TODO: Unnecessary allocation
        Attribute{ pos: self.pos, name: self.name, kind }
    }
}

impl<'a> Source<'a> {
    fn new(schema: &'a Schema, dsl_source: &dsl::Source) -> Result<Source<'a>, &'static str> {
        match dsl_source {
            dsl::Source::TableScan(name) => {
                let rel = schema.find_relation(name).ok_or("No such table")?;
                let attributes = rel.attributes().iter().enumerate()
                    .map(|(i, (name, kind))| Attribute::named(i, name).with_type(*kind)).collect();
                Ok(Source{ attributes, contents: Contents::TableScan(rel) })
            },

            dsl::Source::Tuple(values) => {
                let attributes = values.iter().enumerate().map(|(i, val)| Attribute::numbered(i).guess_type(val)).collect();
                Ok(Source { attributes, contents: Contents::Tuple(values.to_vec()) })
            }
        }
    }

    fn validate_with_finisher(self, finisher: &Finisher) -> Result<Source<'a>, &'static str> {
        match finisher {
            Finisher::Insert(rel) => {
                let target_types = rel.types();
                if target_types.len() != self.attributes.len() {
                    return Err("Number of attributes in the tuple doesn't match the target table");
                }

                if zip(&self.attributes, target_types).any(|(attr, expected)| attr.kind != expected) {
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
    #[default]
    Nil,
}

pub(crate) fn compute_plan<'a>(schema: &'a Schema, query: &dsl::Query) -> Result<Plan<'a>, &'static str> {
    let plan: Plan = compute_source(schema, &query.source)?;

    let plan = compute_finisher(plan, schema, &query.finisher)?;
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
        if let Some(attr) = attributes.iter().find(|attr| attr.name == filter_left(dsl_filter)) {
            let op = filter_operator(dsl_filter);
            filters.push(Filter{
                attribute: attr.clone(),
                right: right_as_cell(dsl_filter),
                comp: Box::new(move |a, b| match op {
                    dsl::Operator::EQ => a == b,
                    dsl::Operator::GT => a > b,
                    dsl::Operator::GE => a >= b,
                    dsl::Operator::LT => a < b,
                    dsl::Operator::LE => a <= b,
                })
            });
        } else {
            return Err("Column not found");
        }
    }

    Ok(Plan{ filters, ..plan })
}

fn compute_finisher<'a>(plan: Plan<'a>, schema: &'a Schema, dsl_finisher: &dsl::Finisher) -> Result<Plan<'a>, &'static str> {
    match dsl_finisher {
        dsl::Finisher::Insert(name) => schema.find_relation(name).map(Finisher::Insert).ok_or("No such target table"),
        dsl::Finisher::AllColumns => Ok(Finisher::Return),
        dsl::Finisher::Count => Ok(Finisher::Count),
        _ => todo!(),
    }.map(|finisher| Plan{ finisher, ..plan })
}

fn right_as_cell(dsl_filter: &dsl::Filter) -> Cell {
    let right = match dsl_filter {
        dsl::Filter::Condition(_, _, right) => right
    };
    
    Cell::from_string(right)

}

fn filter_operator(filter: &dsl::Filter) -> dsl::Operator {
    match filter {
        dsl::Filter::Condition(_, op, _) => *op
    }
}

fn filter_left(filter: &dsl::Filter) -> &str {
    match filter {
        dsl::Filter::Condition(left, _, _) => left
    }
}

fn compute_joins<'a>(plan: Plan<'a>, schema: &Schema, query: &dsl::Query) -> Result<Plan<'a>, &'static str> {
    if query.join_sources.is_empty() {
        return Ok(plan);
    }

    let mut joins = Vec::with_capacity(query.join_sources.len());
    let joinee_table = match &query.source {
        dsl::Source::TableScan(name) => Some(name),
        _ => todo!(),
    }.and_then(|name| schema.find_relation(name)).ok_or("No such table")?;
    
    for join_source in &query.join_sources {
        let joiner_table = match schema.find_relation(&join_source.table) {
            Some(table) => table,
            None => return Err("No such table")
        };

        let joinee_attributes = table_attributes(joinee_table);
        let joiner_attributes = table_attributes(joiner_table);
        if let Some(joinee_key) = joinee_attributes.iter().enumerate().find(|(_, a)| a.name == join_source.left).map(|(i,_)| i) {
            if let Some(joiner_key) = joiner_attributes.iter().enumerate().find(|(_, a)| a.name == join_source.right).map(|(i,_)| i) {
                joins.push(Join{
                    table: joiner_table.name.to_string(),
                    joinee_attributes, joiner_attributes,
                    joinee_key, joiner_key
                })
            } else {
                return Err("Joiner: no such column")
            }
        } else {
            return Err("Joiee: no such column")
        }
    }

    Ok(Plan{ joins, ..plan })
}

fn table_attributes(table: &Relation) -> Vec<Attribute> {
    table.columns().enumerate()
        .map(|(pos, (col_name, kind))| Attribute{ pos: pos, name: col_name.to_string(), kind })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dsl::Operator::{EQ, GT};
    use crate::schema::{Column, Type};

    #[test]
    fn test_compute_filter() {
        let mut schema = Schema::default();
        schema.add_relation("example", &[col("id", Type::NUMBER), col("content", Type::TEXT), col("type", Type::NUMBER)]);

        let filters = expect_filters(compute_plan(&schema, &dsl::Query::scan("example").filter("example.id", EQ, "1").filter("example.type", EQ, "2")), 2);
        assert_eq!(filters.get(0).map(|x| x.attribute.pos), Some(0));
        assert_eq!(filters.get(1).map(|x| x.attribute.pos), Some(2));

        let failure = compute_plan(&schema, &dsl::Query::scan("example").filter("not-a-column", EQ, "0"));
        assert!(failure.is_err());
    }

    #[test]
    fn test_apply_filter() {
        let mut schema = Schema::default();
        schema.add_relation("example", &[col("id", Type::NUMBER), col("content", Type::TEXT), col("type", Type::NUMBER)]);
        let tuple_1 = MockTuple(vec![Cell::from_number(1), Cell::from_string("content1"), Cell::from_number(11)]);
        let tuple_2 = MockTuple(vec![Cell::from_number(2), Cell::from_string("content2"), Cell::from_number(12)]);

        let id_filter = expect_filter(compute_plan(&schema, &dsl::Query::scan("example").filter("example.id", EQ, "1")));
        assert!(id_filter.matches_tuple(&tuple_1));
        assert!(!id_filter.matches_tuple(&tuple_2));

        let content_filter = expect_filter(compute_plan(&schema, &dsl::Query::scan("example").filter("example.content", EQ, "content1")));
        assert!(content_filter.matches_tuple(&tuple_1));
        assert!(!content_filter.matches_tuple(&tuple_2));

        let comp_filter = expect_filter(compute_plan(&schema, &dsl::Query::scan("example").filter("example.id", GT, "1")));
        assert!(!comp_filter.matches_tuple(&tuple_1));
        assert!(comp_filter.matches_tuple(&tuple_2));
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
        schema.add_relation("document", &[col("name", Type::TEXT), col("type_id", Type::NUMBER)]);
        schema.add_relation("type", &[col("id", Type::TEXT), col("name", Type::NUMBER)]);

        let joins = expect_join(compute_plan(&schema, &dsl::Query::scan("document").join("type", "document.type_id", "type.id")));
        assert_eq!(joins.source_table(), "type");
        assert_eq!(attribute_names(&joins.attributes_after()), vec!["document.name", "document.type_id", "type.id", "type.name"]);

        let missing_source_table = compute_plan(&schema, &dsl::Query::scan("nothing").join("type", "a", "b"));
        assert!(missing_source_table.is_err());

        let missing_table = compute_plan(&schema, &dsl::Query::scan("document").join("nothing", "a", "b"));
        assert!(missing_table.is_err());

        let missing_column = compute_plan(&schema, &dsl::Query::scan("document").join("type", "something", "else"));
        assert!(missing_column.is_err());
    }

    #[test]
    fn test_join_match_tuple() {
        let mut schema = Schema::default();
        schema.add_relation("document", &[col("name", Type::TEXT), col("type_id", Type::NUMBER)]);
        schema.add_relation("type", &[col("id", Type::TEXT), col("name", Type::NUMBER)]);
        let document_1 = MockTuple(vec![Cell::from_number(1), Cell::from_number(2)]);
        let document_2 = MockTuple(vec![Cell::from_number(2), Cell::from_number(3)]);
        let type_1 = MockTuple(vec![Cell::from_number(1), Cell::from_string("a")]);
        let type_2 = MockTuple(vec![Cell::from_number(2), Cell::from_string("b")]);

        let join = expect_join(compute_plan(&schema, &dsl::Query::scan("document").join("type", "document.type_id", "type.id")));
        assert_eq!(join.find_match(&[type_1.clone(), type_2.clone()], &document_1), Some(&type_2));
        assert_eq!(join.find_match(&[type_1.clone(), type_2.clone()], &document_2), None);
    }

    #[test]
    fn test_filter_after_join() {
        let mut schema = Schema::default();
        schema.add_relation("document", &[col("name", Type::TEXT), col("type_id", Type::NUMBER)]);
        schema.add_relation("type", &[col("id", Type::TEXT), col("name", Type::NUMBER)]);

        let filter = expect_filter(compute_plan(&schema, &dsl::Query::scan("document").join("type", "document.type_id", "type.id").filter("type.name", EQ, "b")));
        assert_eq!(filter.attribute.pos, 3);
    }

    fn expect_join(result: Result<Plan, &str>) -> Join {
        let plan = result.unwrap();
        assert_eq!(plan.joins.len(), 1);
        plan.joins.into_iter().next().unwrap()
    }

    #[test]
    fn test_source_types_for_select() {
        let mut schema = Schema::default();
        schema.add_relation("example", &[col("id", Type::NUMBER), col("content", Type::TEXT), col("type", Type::NUMBER)]);

        let result = compute_plan(&schema, &dsl::Query::scan("example")).unwrap();
        assert_eq!(source_attributes(&result.source), vec![Type::NUMBER, Type::TEXT, Type::NUMBER]);
        assert_eq!(attribute_names(&result.final_attributes()), vec!["example.id", "example.content", "example.type"]);
    }

    #[test]
    fn test_source_types_for_insert() {
        let mut schema = Schema::default();
        schema.add_relation("example", &[col("id", Type::NUMBER), col("content", Type::TEXT), col("title", Type::TEXT)]);

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
        schema.add_relation("example", &[col("id", Type::NUMBER), col("content", Type::TEXT)]);

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

    fn col(name: &str, kind: Type) -> Column {
        Column{ name: name.to_string(), kind }
    }

    fn source_attributes(source: &Source) -> Vec<Type> {
        source.attributes.iter().map(|attr| attr.kind).collect()
    }

    fn attribute_names(attrs: &[Attribute]) -> Vec<&str> {
        attrs.iter().map(|attr| attr.name.as_str()).collect()
    }

    #[derive(Clone, Debug, PartialEq)]
    struct MockTuple(Vec<Cell>);
    impl Tuple for MockTuple {
        fn cell(&self, attr: &Attribute) -> Cell {
            self.0.get(attr.pos).unwrap().clone()
        }

        fn all_cells(&self, _: &[Attribute]) -> Vec<Cell> {
            self.0.to_vec()
        }
    }
}
