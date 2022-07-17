mod object;
mod schema;

use object::{write_object, read_object};
use rqldb::Database;
use schema::{write_schema, read_schema};

use std::cmp::min;
use std::fs::File;
use std::io::{self, Cursor};
use std::io::BufRead;
use std::io::BufReader;
use std::io::Read;
use std::io::Write;

pub enum Error {
    IOError(io::Error),
}

trait Persist {
    fn write<W: Write>(db: &Database) -> Result<(), Error>;
    fn read<R: Read>() -> Result<Database, Error>;
}

#[allow(dead_code)]
struct FilePersist {
    file: File,
}

pub fn write_db<W: Write>(writer: &mut W, db: &Database) {
    write_schema(writer, &db.schema());
    for o in db.raw_objects() {
       write_object(writer, o.raw_tuples());
    }
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

    let mut object_id = 0usize;
    while reader.has_some() {
        db.recover_object(object_id, read_object(&mut reader));
        object_id += 1;
    }

    db
}

pub(crate) struct ByteReader<R: Read> {
    reader: BufReader<R>,
}

impl<R: Read> ByteReader<R> {
    pub(crate) fn new(reader: R) -> Self {
        Self{ reader: BufReader::new(reader) }
    }

    pub(crate) fn read_u8(&mut self) -> io::Result<u8> {
        let buf = self.reader.fill_buf()?;
        let x = buf[0];
        self.reader.consume(1);
        Ok(x)
    }

    pub(crate) fn peek_u8(&mut self) -> io::Result<u8> {
        let buf = self.reader.fill_buf()?;
        Ok(buf[0])
    }

    pub(crate) fn read_bytes(&mut self, size: usize) -> io::Result<Vec<u8>> {
        let mut out: Vec<u8> = Vec::with_capacity(size);
        while out.len() < size {
            out.extend(self.reader.fill_buf().unwrap());
            self.reader.consume(min(size, out.len()));
        }

        Ok(out.into_iter().take(size).collect())
    }

    pub(crate) fn has_some(&mut self) -> bool {
        println!("fill_buf() {:?}", self.reader.fill_buf().unwrap());
        !self.reader.fill_buf().unwrap().is_empty()
    }
}

pub(crate) type ByteTuple = Vec<Vec<u8>>;

#[cfg(test)]
mod tests {
    use rqldb::{Command, Type, Query};

    use super::*;

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
        let result = saved_db.execute_query(&Query::scan("example")).unwrap();
        assert_eq!(result.size(), 2);
    }
}
