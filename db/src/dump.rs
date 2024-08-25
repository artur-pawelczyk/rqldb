use core::fmt;
use std::collections::BTreeMap;

use crate::{schema::Relation, Definition, Query, QueryResults};

pub(crate) fn dump_create(rel: &Relation) -> Definition {
    rel.attributes().fold(Definition::relation(rel.name()), |acc, col| {
        if col.indexed() {
            acc.indexed_attribute(col.short_name(), col.kind())
        } else {
            acc.attribute(col.short_name(), col.kind())
        }
    })
}

pub(crate) fn dump_values<'a>(rel: &str, values: QueryResults, writer: &mut impl fmt::Write) {
    for tuple in values.tuples() {
        let mut map = BTreeMap::new();
        for attr in tuple.attributes() {
            map.insert(attr.name(), tuple.element(attr.name()).unwrap().to_string());
        }

        let insert_query = Query::tuple(&map).insert_into(rel);
        writeln!(writer, ".insert {}", insert_query).unwrap();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{schema::Schema, Type, Definition, Database, Query};

    #[test]
    fn test_dump_schema() {
        let mut schema = Schema::default();
        schema.create_table("example")
            .indexed_column("id", Type::NUMBER)
            .column("content", Type::TEXT)
            .add();

        let rel = schema.find_relation("example").unwrap();

        assert_eq!(dump_create(rel), Definition::relation("example")
                   .indexed_attribute("id", Type::NUMBER)
                   .attribute("content", Type::TEXT));
    }

    #[test]
    fn test_dump_contents() {
        let mut db = Database::default();

        db.define(&Definition::relation("example")
                          .indexed_attribute("id", Type::NUMBER)
                          .attribute("content", Type::TEXT));

        let mut expected_tuple = BTreeMap::new();
        expected_tuple.insert("example.id", "1");
        expected_tuple.insert("example.content", "first");
        let creation_query = Query::tuple(&expected_tuple).insert_into("example");
        db.insert(&creation_query).unwrap();

        let mut dump = String::new();
        dump_values("example", db.execute_query(&Query::scan("example")).unwrap(), &mut dump);
        assert_eq!(dump.trim(), format!(".insert {}", creation_query));
    }
}
