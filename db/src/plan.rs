use crate::Cell;
use crate::dsl;
use crate::schema::{Schema, Relation};
use crate::db::Tuple;

#[derive(Default)]
pub struct Plan {
    pub filters: Vec<Filter>,
    pub joins: Vec<Join>,
    pub final_attributes: Vec<String>,
}

pub struct Filter {
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

pub struct Join {
    table: String,
    joinee_key_pos: u32,
    joiner_key_pos: u32,
    attributes: Vec<String>,
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

    pub fn take_attributes(self) -> Vec<String> {
        self.attributes
    }
}

pub fn compute_plan(schema: &Schema, query: &dsl::Query) -> Result<Plan, &'static str> {
    let rel = match &query.source {
        dsl::Source::TableScan(name) => if let Some(rel) = schema.find_relation(name) {
            rel
        } else {
            return Err("No such table");
        }
        _ => { return Ok(Plan::default()) },
    };


    let joins = compute_joins(schema, query)?;

    let default_attributes = rel.full_attribute_names();
    let final_attributes = joins.iter().last().map_or(default_attributes, |join| join.attributes.to_vec());

    let filters = compute_filters(schema, query)?;

    Ok(Plan {
        joins, filters,
        final_attributes
    })
}

fn compute_filters(schema: &Schema, query: &dsl::Query) -> Result<Vec<Filter>, &'static str> {
    if query.filters.is_empty() {
        return Ok(vec![]);
    }

    let rel = match &query.source {
        dsl::Source::TableScan(name) => schema.find_relation(name).unwrap(),
        _ => todo!(),
    };

    let default_attributes = rel.full_attribute_names();
    let joins = compute_joins(schema, query)?;
    let attributes = joins.iter().last().map_or(&default_attributes, |join| &join.attributes);

    let mut filters = Vec::with_capacity(query.filters.len());
    for dsl_filter in &query.filters {
        if let Some(pos) = attributes.iter().enumerate().find(|(_, attr)| attr.as_str() == filter_left(dsl_filter)).map(|(i, _)| i) {
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

    Ok(filters)
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

fn compute_joins(schema: &Schema, query: &dsl::Query) -> Result<Vec<Join>, &'static str> {
    if query.join_sources.is_empty() {
        return Ok(vec![]);
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

    Ok(joins)
}

fn compute_attributes(joinee: &Relation, joiner: &Relation) -> Vec<String> {
    let mut combined = joinee.full_attribute_names();
    combined.extend(joiner.full_attribute_names());
    combined
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

        let filters = expect_filters(compute_filters(&schema, &dsl::Query::scan("example").filter("example.id", EQ, "1").filter("example.type", EQ, "2")), 2);
        assert_eq!(filters.get(0).map(|x| x.cell_pos), Some(0));
        assert_eq!(filters.get(1).map(|x| x.cell_pos), Some(2));

        let failure = compute_filters(&schema, &dsl::Query::scan("example").filter("not-a-column", EQ, "0"));
        assert!(failure.is_err());
    }

    #[test]
    fn test_apply_filter() {
        let mut schema = Schema::default();
        schema.add_relation("example", &[col("id", Type::NUMBER), col("content", Type::TEXT), col("type", Type::NUMBER)]);
        let tuple_1 = MockTuple(vec![Cell::from_number(1), Cell::from_string("content1"), Cell::from_number(11)]);
        let tuple_2 = MockTuple(vec![Cell::from_number(2), Cell::from_string("content2"), Cell::from_number(12)]);

        let id_filter = expect_filter(compute_filters(&schema, &dsl::Query::scan("example").filter("example.id", EQ, "1")));
        assert!(id_filter.matches_tuple(&tuple_1));
        assert!(!id_filter.matches_tuple(&tuple_2));

        let content_filter = expect_filter(compute_filters(&schema, &dsl::Query::scan("example").filter("example.content", EQ, "content1")));
        assert!(content_filter.matches_tuple(&tuple_1));
        assert!(!content_filter.matches_tuple(&tuple_2));

        let comp_filter = expect_filter(compute_filters(&schema, &dsl::Query::scan("example").filter("example.id", GT, "1")));
        assert!(!comp_filter.matches_tuple(&tuple_1));
        assert!(comp_filter.matches_tuple(&tuple_2));
    }

    fn expect_filters(result: Result<Vec<Filter>, &'static str>, n: usize) -> Vec<Filter> {
        match result {
            Ok(filters) => {
                assert_eq!(filters.len(), n);
                filters
            },
            Err(msg) => panic!("{}", msg)
        }
    }

    fn expect_filter(result: Result<Vec<Filter>, &'static str>) -> Filter {
        let filters = expect_filters(result, 1);
        filters.into_iter().next().unwrap()
    }

    #[test]
    fn test_compute_join() {
        let mut schema = Schema::default();
        schema.add_relation("document", &[col("name", Type::TEXT), col("type_id", Type::NUMBER)]);
        schema.add_relation("type", &[col("id", Type::TEXT), col("name", Type::NUMBER)]);

        let joins = expect_join(compute_joins(&schema, &dsl::Query::scan("document").join("type", "document.type_id", "type.id")));
        assert_eq!(joins.source_table(), "type");
        assert_eq!(joins.attributes, vec!["document.name", "document.type_id", "type.id", "type.name"]);

        let missing_source_table = compute_joins(&schema, &dsl::Query::scan("nothing").join("type", "a", "b"));
        assert!(missing_source_table.is_err());

        let missing_table = compute_joins(&schema, &dsl::Query::scan("document").join("nothing", "a", "b"));
        assert!(missing_table.is_err());

        let missing_column = compute_joins(&schema, &dsl::Query::scan("document").join("type", "something", "else"));
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

        let join = expect_join(compute_joins(&schema, &dsl::Query::scan("document").join("type", "document.type_id", "type.id")));
        assert_eq!(join.find_match(&[type_1.clone(), type_2.clone()], &document_1), Some(&type_2));
        assert_eq!(join.find_match(&[type_1.clone(), type_2.clone()], &document_2), None);
    }

    #[test]
    fn test_filter_after_join() {
        let mut schema = Schema::default();
        schema.add_relation("document", &[col("name", Type::TEXT), col("type_id", Type::NUMBER)]);
        schema.add_relation("type", &[col("id", Type::TEXT), col("name", Type::NUMBER)]);

        let filter = expect_filter(compute_filters(&schema, &dsl::Query::scan("document").join("type", "document.type_id", "type.id").filter("type.name", EQ, "b")));
        assert_eq!(filter.cell_pos, 3);
    }

    fn expect_join(result: Result<Vec<Join>, &str>) -> Join {
        let joins = result.unwrap();
        assert_eq!(joins.len(), 1);
        joins.into_iter().next().unwrap()
    }

    fn col(name: &str, kind: Type) -> Column {
        Column{ name: name.to_string(), kind }
    }

    #[derive(Clone, Debug, PartialEq)]
    struct MockTuple(Vec<Cell>);
    impl Tuple for MockTuple {
        fn cell_at(&self, pos: u32) -> Option<&Cell> {
            self.0.get(pos as usize)
        }
    }
}
