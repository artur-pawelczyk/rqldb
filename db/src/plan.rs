use crate::dsl;
use crate::dsl::Operator::EQ;
use crate::schema::{Column, Schema, Type, Relation};

pub struct Filter {
    cell_pos: u32,
}

pub fn compute_filters(schema: &Schema, query: &dsl::Query) -> Result<Vec<Filter>, &'static str> {
    let rel = match &query.source {
        dsl::Source::TableScan(name) => schema.find_relation(&name).unwrap(),
        _ => todo!(),
    };

    let mut filters = Vec::with_capacity(query.filters.len());
    for dsl_filter in &query.filters {
        if let Some(pos) = find_left_position(rel, &dsl_filter) {
            filters.push(Filter{ cell_pos: pos });
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_compute_filter() {
        let mut schema = Schema::default();
        schema.add_relation("example", &[col("id", Type::NUMBER), col("content", Type::TEXT), col("type", Type::NUMBER)]);

        let mut result = compute_filters(&schema, &dsl::Query::scan("example").filter("id", EQ, "1").filter("type", EQ, "2"));
        let filters = result.unwrap();
        assert_eq!(filters.get(0).map(|x| x.cell_pos), Some(0));
        assert_eq!(filters.get(1).map(|x| x.cell_pos), Some(2));

        result = compute_filters(&schema, &dsl::Query::scan("example").filter("not-a-column", EQ, "0"));
        assert!(result.is_err());
    }

    fn col(name: &str, kind: Type) -> Column {
        Column{ name: name.to_string(), kind }
    }
}
