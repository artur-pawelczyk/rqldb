use crate::Cell;
use crate::dsl;
use crate::schema::{Schema, Relation};
use crate::db::TupleTrait;

pub struct Filter {
    cell_pos: u32,
    right: Cell,
}

impl Filter {
    pub fn matches_tuple(&self, tuple: &impl TupleTrait) -> bool {
        let left = tuple.cell_at(self.cell_pos).expect("Already validated");
        left == &self.right
    }
}

pub fn compute_filters(schema: &Schema, query: &dsl::Query) -> Result<Vec<Filter>, &'static str> {
    if query.filters.is_empty() {
        return Ok(vec![]);
    }

    let rel = match &query.source {
        dsl::Source::TableScan(name) => schema.find_relation(&name).unwrap(),
        _ => todo!(),
    };

    let mut filters = Vec::with_capacity(query.filters.len());
    for dsl_filter in &query.filters {
        if let Some(pos) = find_left_position(rel, &dsl_filter) {
            filters.push(Filter{ cell_pos: pos, right: right_as_cell(dsl_filter) });
        } else {
            return Err("Column not found");
        }
    }

    Ok(filters)
}

fn find_left_position(rel: &Relation, dsl_filter: &dsl::Filter) -> Option<u32> {
    match dsl_filter {
        dsl::Filter::Condition(left, _, _) => rel.column_position(left)
    }
}

fn right_as_cell(dsl_filter: &dsl::Filter) -> Cell {
    let as_num: i32 = match dsl_filter {
        dsl::Filter::Condition(_, _, right) => right.parse().unwrap()
    };

    Cell::from_number(as_num)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dsl::Operator::EQ;
    use crate::schema::{Column, Type};

    #[test]
    fn test_compute_filter() {
        let mut schema = Schema::default();
        schema.add_relation("example", &[col("id", Type::NUMBER), col("content", Type::TEXT), col("type", Type::NUMBER)]);

        let filters = expect_filters(compute_filters(&schema, &dsl::Query::scan("example").filter("id", EQ, "1").filter("type", EQ, "2")), 2);
        assert_eq!(filters.get(0).map(|x| x.cell_pos), Some(0));
        assert_eq!(filters.get(1).map(|x| x.cell_pos), Some(2));

        let failure = compute_filters(&schema, &dsl::Query::scan("example").filter("not-a-column", EQ, "0"));
        assert!(failure.is_err());
    }

    #[test]
    fn test_apply_filter() {
        let mut schema = Schema::default();
        schema.add_relation("example", &[col("id", Type::NUMBER), col("content", Type::TEXT), col("type", Type::NUMBER)]);

        let filter = expect_filter(compute_filters(&schema, &dsl::Query::scan("example").filter("id", EQ, "1")));
        let matching_tuple = MockTuple(vec![Cell::from_number(1)]);
        let not_matching_tuple = MockTuple(vec![Cell::from_number(2)]);

        assert!(filter.matches_tuple(&matching_tuple));
        assert!(!filter.matches_tuple(&not_matching_tuple));
    }

    fn expect_filters(result: Result<Vec<Filter>, &'static str>, n: usize) -> Vec<Filter> {
        assert!(result.is_ok());
        let filters = result.unwrap();
        assert_eq!(filters.len(), n);
        filters
    }

    fn expect_filter(result: Result<Vec<Filter>, &'static str>) -> Filter {
        let filters = expect_filters(result, 1);
        filters.into_iter().next().unwrap()
    }

    fn col(name: &str, kind: Type) -> Column {
        Column{ name: name.to_string(), kind }
    }

    struct MockTuple(Vec<Cell>);

    impl TupleTrait for MockTuple {
        fn cell_at(&self, pos: u32) -> Option<&Cell> {
            self.0.get(pos as usize)
        }
    }
}
