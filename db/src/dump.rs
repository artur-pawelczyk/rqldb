use core::fmt;
use std::collections::BTreeMap;

use crate::{schema::Relation, Command, Query, QueryResults};

pub(crate) fn dump_create(rel: &Relation) -> Command {
    rel.attributes().fold(Command::create_table(rel.name()), |acc, col| {
        if col.indexed() {
            acc.indexed_column(col.short_name(), col.kind())
        } else {
            acc.column(col.short_name(), col.kind())
        }
    })
}

pub(crate) fn dump_values<'a>(rel: &str, values: QueryResults, writer: &mut impl fmt::Write) {
    let attributes = values.attributes();
    for tuple in values.tuples() {
        let mut map = BTreeMap::new();
        for attr in attributes {
            map.insert(attr.as_str(), tuple.cell_by_name(attr).unwrap().as_string());
        }

        let insert_query = Query::tuple(&map).insert_into(rel);
        write!(writer, "{}\n", insert_query).unwrap();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{schema::Schema, Type, Command, Database, Query};

    #[test]
    fn test_dump_schema() {
        let mut schema = Schema::default();
        schema.create_table("example")
            .indexed_column("id", Type::NUMBER)
            .column("content", Type::TEXT)
            .add();

        let rel = schema.find_relation("example").unwrap();

        assert_eq!(dump_create(rel), Command::create_table("example")
                   .indexed_column("id", Type::NUMBER)
                   .column("content", Type::TEXT));
    }

    #[test]
    fn test_dump_contents() {
        let mut db = Database::default();

        db.execute_create(&Command::create_table("example")
                          .indexed_column("id", Type::NUMBER)
                          .column("content", Type::TEXT));

        let mut expected_tuple = BTreeMap::new();
        expected_tuple.insert("example.id", "1");
        expected_tuple.insert("example.content", "first");
        let creation_query = Query::tuple(&expected_tuple).insert_into("example");
        db.execute_query(&creation_query).unwrap();

        let mut dump = String::new();
        dump_values("example", db.execute_query(&Query::scan("example")).unwrap(), &mut dump);
        assert_eq!(dump.trim(), creation_query.to_string());
    }
}
