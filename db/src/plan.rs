use crate::dsl;
use crate::schema::{Column, Schema, Type};

pub struct Filter {
    cell_pos: u32,
}

pub fn compute_filters(schema: &Schema, query: &dsl::Query) -> Vec<Filter> {
    let rel = match &query.source {
        dsl::Source::TableScan(name) => schema.find_relation(&name).unwrap(),
        _ => todo!(),
    };

    query.filters.iter().map(|dsl_filter| {
        let pos = match dsl_filter {
            dsl::Filter::Condition(left, _, _) => rel.column_position(left)
        };

        Filter{ cell_pos: pos.unwrap() }
    }).collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_compute_filter() {
        let mut schema = Schema::default();
        schema.add_relation("example", &[col("id", Type::NUMBER), col("content", Type::TEXT)]);
        let query = dsl::Query::scan("example").filter("id", dsl::Operator::EQ, "1");

        let filters = compute_filters(&schema, &query);
        assert_eq!(filters.get(0).map(|x| x.cell_pos), Some(0));
    }

    fn col(name: &str, kind: Type) -> Column {
        Column{ name: name.to_string(), kind }
    }
}
