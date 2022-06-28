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
}

impl<'a> Plan<'a> {
    fn validate_with_finisher(self, finisher: &Finisher) -> Result<Self, &'static str> {
        Ok(Plan{
            source: self.source.validate_with_finisher(finisher)?,
            ..self
        })
    }

    pub fn final_attributes(&self) -> &[Attribute] {
        if let Some(last_join) = self.joins.iter().last() {
            &last_join.attributes
        } else {
            &self.source.attributes
        }
    }
}

pub(crate) struct Filter {
    cell_pos: u32,
    right: Cell,
    comp: Box<dyn Fn(&Cell, &Cell) -> bool>,
}

impl Filter {
    pub fn matches_tuple(&self, tuple: &impl Tuple) -> bool {
        let left = tuple.cell_at(self.cell_pos).expect("Already validated");
        (self.comp)(left, &self.right)
    }
}

pub(crate) struct Join {
    table: String,
    joinee_key_pos: u32,
    joiner_key_pos: u32,
    attributes: Vec<Attribute>,
}

impl Join {
    pub fn find_match<'a, T: Tuple>(&self, joiner_tuples: &'a [T], joinee: &impl Tuple) -> Option<&'a T> {
        let joinee_key = joinee.cell_at(self.joinee_key_pos);
        assert!(joinee_key.is_some());

        joiner_tuples.iter()
            .find(|tuple| tuple.cell_at(self.joiner_key_pos) == joinee_key)
    }

    pub fn source_table(&self) -> &str {
        &self.table
    }
}

pub(crate) struct Attribute {
    pub pos: u32,
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
    fn numbered(num: usize) -> Attribute {
        Attribute{ pos: num as u32, name: num.to_string(), kind: Type::default() }
    }

    fn named(pos: u32, name: &str) -> Attribute {
        Attribute{ pos, name: name.to_string(), kind: Type::default() }
    }

    fn with_type(self, kind: Type) -> Attribute {
        Attribute{ pos: self.pos, name: self.name, kind: kind }
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
                    .map(|(i, (name, kind))| Attribute::named(i as u32, name).with_type(*kind)).collect();
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

            Finisher::Return => {
                Ok(Source{ attributes: self.attributes, contents: self.contents })
            }
        }
    }
}

enum Finisher<'a> {
    Return,
    Insert(&'a Relation)
}

pub(crate) fn compute_plan<'a>(schema: &'a Schema, query: &dsl::Query) -> Result<Plan<'a>, &'static str> {
    let plan: Plan = compute_source(schema, &query.source)?;

    let finisher = compute_finisher(schema, &query.finisher)?;
    let plan = plan.validate_with_finisher(&finisher)?;

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
        if let Some(pos) = attributes.iter().enumerate().find(|(_, attr)| attr.name == filter_left(dsl_filter)).map(|(i, _)| i) {
            let op = filter_operator(dsl_filter);
            filters.push(Filter{
                cell_pos: pos as u32,
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

fn compute_finisher<'a>(schema: &'a Schema, dsl_finisher: &dsl::Finisher) -> Result<Finisher<'a>, &'static str> {
    match dsl_finisher {
        dsl::Finisher::Insert(name) => schema.find_relation(name).map(Finisher::Insert).ok_or("No such target table"),
        _ => Ok(Finisher::Return),
    }
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

        if let Some(joinee_key_pos) = joinee_table.column_position(&join_source.left) {
            if let Some(joiner_key_pos) = joiner_table.column_position(&join_source.right) {
                joins.push(Join{
                    table: joiner_table.name.to_string(),
                    joinee_key_pos, joiner_key_pos,
                    attributes: compute_attributes(joinee_table, joiner_table),
                })
            } else {
                return Err("Join: no such column")
            }
        } else {
            return Err("Join: no such column")
        }
    }

    Ok(Plan{ joins, ..plan })
}

fn compute_attributes(joinee: &Relation, joiner: &Relation) -> Vec<Attribute> {
    let mut combined = joinee.attributes();
    combined.extend(joiner.attributes());
    combined.into_iter().enumerate().map(|(i, (name, kind))| Attribute{ pos: i as u32, name, kind }).collect()
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
        assert_eq!(filters.get(0).map(|x| x.cell_pos), Some(0));
        assert_eq!(filters.get(1).map(|x| x.cell_pos), Some(2));

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
        assert_eq!(attribute_names(&joins.attributes), vec!["document.name", "document.type_id", "type.id", "type.name"]);

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
        assert_eq!(filter.cell_pos, 3);
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
        fn cell_at(&self, pos: u32) -> Option<&Cell> {
            self.0.get(pos as usize)
        }

        fn into_cells(self) -> Vec<Cell> {
            self.0
        }
    }
}
