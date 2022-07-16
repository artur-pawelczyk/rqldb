use rqldb::schema::{Column, Schema};

use std::io::BufRead;
use std::io::BufReader;
use std::io::Read;
use std::io::Write;

use bson::Document;
use bson::Bson;
use bson::Array;

#[allow(dead_code)]
fn write_schema<W: Write>(writer: &mut W, schema: &Schema) {
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

#[allow(dead_code)]
fn read_schema<R: Read>(reader: R) -> Schema {
    let doc = Document::from_reader(reader).unwrap();
    let mut schema = Schema::default();

    for rel_doc in doc.get_array("relations").unwrap().iter().map(|o| o.as_document().unwrap()) {
        let id = rel_doc.get_i64("id").unwrap();
        let name = rel_doc.get_str("name").unwrap();
        let mut columns = Vec::new();
        for col in rel_doc.get_array("columns").unwrap() {
            let col_doc = col.as_document().unwrap();
            let col = Column::new(col_doc.get_str("name").unwrap(), col_doc.get_str("kind").unwrap().parse().unwrap());

            let col = if col_doc.get_bool("indexed").unwrap() {
                col.indexed()
            } else {
                col
            };

            columns.push(col);
        }

        schema.add_relation(id as usize, name, &columns);
    }

    schema
}

#[allow(dead_code)]
type ByteTuple = Vec<Vec<u8>>;
#[allow(dead_code)]
fn write_object<W: Write>(writer: &mut W, tuples: &[ByteTuple]) {
    writer.write(&[tuples.len() as u8]).unwrap();
    for tuple in tuples {
        writer.write(&[tuple.len() as u8]).unwrap();

        for cell in tuple {
            writer.write(&[cell.len() as u8]).unwrap();
            writer.write(&cell).unwrap();
        }
    }
}

#[allow(dead_code)]
fn read_object<R: Read>(reader: R) -> Vec<ByteTuple> {
    let mut reader = BufReader::new(reader);

    let n_tuples = reader.fill_buf().unwrap()[0] as usize;
    reader.consume(1);

    let mut tuples = Vec::with_capacity(n_tuples);
    for _ in 0..n_tuples {
        let n_cells = reader.fill_buf().unwrap()[0];
        reader.consume(1);

        let mut tuple = Vec::new();
        for _ in 0..n_cells {
            let n_bytes = reader.fill_buf().unwrap()[0] as usize;
            reader.consume(1);
            let cell = reader.fill_buf().unwrap()[0..n_bytes].to_vec();
            reader.consume(n_bytes);
            tuple.push(cell);
        }

        tuples.push(tuple);
    }

    tuples
}

#[cfg(test)]
mod tests {
    use std::{io::Cursor, iter::zip};
    use rqldb::{schema::Type, Database, Query, Command};

    use super::*;

    #[test]
    fn test_serialize_schema() {
        let mut schema = Schema::default();
        schema.add_relation(0, "example", &[Column::new("id", Type::NUMBER).indexed(), Column::new("content", Type::TEXT)]);

        let mut out = Vec::new();
        write_schema(&mut out, &schema);

        let saved_schema = read_schema(Cursor::new(out));

        for (rel, saved_rel) in zip(schema.relations, saved_schema.relations) {
            assert_eq!(rel.id, saved_rel.id);
            assert_eq!(rel.name, saved_rel.name);
            assert_eq!(rel.columns, saved_rel.columns);
        }
    }

    #[test]
    fn test_serialize_empty_object() {
        let mut db = Database::default();
        db.execute_create(&Command::create_table("example").indexed_column("id", Type::NUMBER).column("content", Type::TEXT));

        let raw_object = db.raw_object("example").unwrap();
        let mut out = Vec::new();
        write_object(&mut out, raw_object.raw_tuples());
        let saved_object = read_object(Cursor::new(out));
        assert_eq!(raw_object.raw_tuples(), &saved_object);
    }

    #[test]
    fn test_serialize_object() {
        let mut db = Database::default();
        db.execute_create(&Command::create_table("example").indexed_column("id", Type::NUMBER).column("content", Type::TEXT));

        db.execute_query(&Query::tuple(&["1", "test"]).insert_into("example")).unwrap();
        db.execute_query(&Query::tuple(&["2", "test"]).insert_into("example")).unwrap();

        let raw_object = db.raw_object("example").unwrap();
        let mut out = Vec::new();
        write_object(&mut out, raw_object.raw_tuples());
        let saved_object = read_object(Cursor::new(out));
        assert_eq!(raw_object.raw_tuples(), &saved_object);
    }
}