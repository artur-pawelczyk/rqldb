use std::io::{Read, Write};

use bson::{Array, Document, Bson};
use rqldb::{schema::Schema, Type};

pub(crate) fn write_schema<W: Write>(writer: &mut W, schema: &Schema) {
    let mut relations = Array::new();
    for rel in &schema.relations {
        let mut rel_doc = Document::new();
        rel_doc.insert("id", rel.id as i64);
        rel_doc.insert("name", rel.name.as_str());

        let mut col_arr = Array::new();
        for col in &rel.columns {
            let mut col_doc = Document::new();
            col_doc.insert("name", col.name.as_str());
            col_doc.insert("kind", col.kind.to_string());
            col_doc.insert("indexed", col.indexed);
            col_arr.push(Bson::Document(col_doc));
        }

        rel_doc.insert("columns", col_arr);

        relations.push(Bson::Document(rel_doc));
    }

    let mut doc = Document::new();
    doc.insert("relations", relations);

    doc.to_writer(writer).unwrap();
}

pub(crate) fn read_schema<R: Read>(reader: R) -> Vec<(usize, String, Vec<Column>)> {
    let doc = Document::from_reader(reader).unwrap();
    let mut tables = Vec::new();

    for rel_doc in doc.get_array("relations").unwrap().iter().map(|o| o.as_document().unwrap()) {
        let id = rel_doc.get_i64("id").unwrap() as usize;
        let name = rel_doc.get_str("name").unwrap();
        let mut columns = Vec::new();
        for col in rel_doc.get_array("columns").unwrap() {
            let col_doc = col.as_document().unwrap();
            let name = col_doc.get_str("name").unwrap();
            let kind = col_doc.get_str("kind").unwrap().parse().unwrap();
            let indexed = col_doc.get_bool("indexed").unwrap();

            columns.push(Column{ name: name.to_string(), kind, indexed });
        }

        tables.push((id, name.to_string(), columns));
    }

    tables
}

pub(crate) struct Column {
    pub(crate) name: String,
    pub(crate) kind: Type,
    pub(crate) indexed: bool,
}

#[cfg(test)]
mod tests {
    use std::{io::Cursor, iter::zip};

    use rqldb::{Type, Database};

    use super::*;

    #[test]
    fn test_serialize_schema() {
        let mut db = Database::default();
        db.create_table("example")
            .indexed_column("id", Type::NUMBER)
            .column("content", Type::TEXT)
            .create();

        let mut out = Vec::new();
        write_schema(&mut out, db.schema());

        let saved_schema = read_schema(Cursor::new(out));

        for (rel, saved_rel) in zip(&db.schema().relations, saved_schema) {
            assert_eq!(rel.id, saved_rel.0);
            assert_eq!(rel.name, saved_rel.1);
            assert_eq!(rel.columns.len(), saved_rel.2.len());
        }
    }


}
