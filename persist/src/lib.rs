use rqldb::{Database, Type};
use rqldb::schema::Schema;

use std::cmp::min;
use std::io::{self, Cursor};
use std::io::BufRead;
use std::io::BufReader;
use std::io::Read;
use std::io::Write;

use bson::Document;
use bson::Bson;
use bson::Array;

pub fn write_db<W: Write>(writer: &mut W, db: &Database) {
    write_schema(writer, &db.schema());
}

pub fn read_db<R: Read>(reader: R) -> Database {
    let mut reader = ByteReader::new(reader);
    let schema_len = reader.peek_u8().unwrap() as usize;
    let schema_bytes = reader.read_bytes(schema_len).unwrap();

    let mut db = Database::default();
    let mut tables = read_schema(Cursor::new(schema_bytes));
    tables.sort_by_key(|x| x.0);
    for table in tables {
        table.2.iter().fold(db.create_table(&table.1), |acc, col| {
            if col.indexed {
                acc.indexed_column(&col.name, col.kind)
            } else {
                acc.column(&col.name, col.kind)
            }
        }).create();
    }

    db
}

struct ByteReader<R: Read> {
    reader: BufReader<R>,
}

impl<R: Read> ByteReader<R> {
    fn new(reader: R) -> Self {
        Self{ reader: BufReader::new(reader) }
    }

    fn read_u8(&mut self) -> io::Result<u8> {
        let buf = self.reader.fill_buf()?;
        let x = buf[0];
        self.reader.consume(1);
        Ok(x)
    }

    fn peek_u8(&mut self) -> io::Result<u8> {
        let buf = self.reader.fill_buf()?;
        Ok(buf[0])
    }

    fn read_bytes(&mut self, size: usize) -> io::Result<Vec<u8>> {
        let mut out: Vec<u8> = Vec::with_capacity(size);
        while out.len() < size {
            out.extend(self.reader.fill_buf().unwrap());
            self.reader.consume(min(size, out.len()));
        }

        Ok(out.into_iter().take(size).collect())
    }
}

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
fn read_schema<R: Read>(reader: R) -> Vec<(usize, String, Vec<Column>)> {
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

struct Column {
    name: String,
    kind: Type,
    indexed: bool,
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
    use rqldb::{schema::{Column, Type}, Query, Command};

    use super::*;

    #[test]
    fn test_serialize_schema() {
        let mut schema = Schema::default();
        schema.add_relation(0, "example", &[Column::new("id", Type::NUMBER).indexed(), Column::new("content", Type::TEXT)]);

        let mut out = Vec::new();
        write_schema(&mut out, &schema);

        let saved_schema = read_schema(Cursor::new(out));

        for (rel, saved_rel) in zip(schema.relations, saved_schema) {
            assert_eq!(rel.id, saved_rel.0);
            assert_eq!(rel.name, saved_rel.1);
            assert_eq!(rel.columns.len(), saved_rel.2.len());
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

    // TODO: test serialization of a table with deleted rows (do a vacuum before serialization)

    #[test]
    fn test_serialize_db() {
        let mut db = Database::default();
        db.execute_create(&Command::create_table("example").indexed_column("id", Type::NUMBER).column("content", Type::TEXT));

        db.execute_query(&Query::tuple(&["1", "test"]).insert_into("example")).unwrap();
        db.execute_query(&Query::tuple(&["2", "test"]).insert_into("example")).unwrap();

        let mut out = Vec::new();
        write_db(&mut out, &db);

        let saved_db = read_db(Cursor::new(out));
        assert!(saved_db.raw_object("example").is_some());
    }
}
