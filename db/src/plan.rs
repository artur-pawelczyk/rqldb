use crate::Cell;
use crate::dsl;
use crate::schema::{Schema, Relation, Type};
use crate::db::TupleTrait;

pub struct Filter {
    cell_pos: u32,
    right: Cell,
    comp: Box<dyn Fn(&Cell, &Cell) -> bool>,
}

impl Filter {
    pub fn matches_tuple(&self, tuple: &impl TupleTrait) -> bool {
        let left = tuple.cell_at(self.cell_pos).expect("Already validated");
        (self.comp)(left, &self.right)
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
            let op = filter_operator(dsl_filter);
            filters.push(Filter{
                cell_pos: pos,
                right: right_as_cell(rel, dsl_filter),
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

fn find_left_position(rel: &Relation, dsl_filter: &dsl::Filter) -> Option<u32> {
    match dsl_filter {
        dsl::Filter::Condition(left, _, _) => rel.column_position(left)
    }
}

fn right_as_cell(rel: &Relation, dsl_filter: &dsl::Filter) -> Cell {
    let (kind, right_str) = match dsl_filter {
        dsl::Filter::Condition(left, _, right) => (rel.column_type(left).unwrap(), right)
    };

    match kind {
        Type::NUMBER => Cell::from_number(right_str.parse().unwrap()),
        Type::TEXT => Cell::from_string(right_str),
        _ => todo!()
    }
}

fn filter_operator(filter: &dsl::Filter) -> dsl::Operator {
    match filter {
        dsl::Filter::Condition(_, op, _) => *op
    }
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

        let filters = expect_filters(compute_filters(&schema, &dsl::Query::scan("example").filter("id", EQ, "1").filter("example.type", EQ, "2")), 2);
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

        let id_filter = expect_filter(compute_filters(&schema, &dsl::Query::scan("example").filter("id", EQ, "1")));
        assert!(id_filter.matches_tuple(&tuple_1));
        assert!(!id_filter.matches_tuple(&tuple_2));

        let content_filter = expect_filter(compute_filters(&schema, &dsl::Query::scan("example").filter("content", EQ, "content1")));
        assert!(content_filter.matches_tuple(&tuple_1));
        assert!(!content_filter.matches_tuple(&tuple_2));

        let comp_filter = expect_filter(compute_filters(&schema, &dsl::Query::scan("example").filter("id", GT, "1")));
        assert!(!comp_filter.matches_tuple(&tuple_1));
        assert!(comp_filter.matches_tuple(&tuple_2));
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
