mod object;
mod schema;

use object::{write_object, read_object};
use rqldb::Database;
use schema::{write_schema, read_schema};

use std::cmp::min;
use std::fs::{File, OpenOptions};
use std::io::{self, Cursor, ErrorKind};
use std::io::BufRead;
use std::io::BufReader;
use std::io::Read;
use std::io::Write;
use std::path::{PathBuf, Path};
use std::error;
use std::fmt;

#[derive(Debug)]
pub enum Error {
    IOError(io::Error),
}

impl From<io::Error> for Error {
    fn from(e: io::Error) -> Error {
        Error::IOError(e)
    }
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let Self::IOError(e) = self;
        write!(f, "IOError: {}", e)
    }
}

impl error::Error for Error {}

// TODO: Split into two traits. "read" should consume self
pub trait Persist {
    fn write(&mut self, db: &Database) -> Result<(), Error>;
    fn read(&self, db: Database) -> Result<Database, Error>;
}

#[allow(dead_code)]
pub struct FilePersist {
    path: PathBuf,
}

impl FilePersist {
    pub fn new(path: &Path) -> Self {
        Self{ path: path.to_path_buf() }
    }
}

impl Persist for FilePersist {
    fn write(&mut self, db: &Database) -> Result<(), Error> {
        let mut file = File::options().create(true).append(true).open(self.path.clone())?;
        write_db(&mut file, &db);
        Ok(())
    }

    fn read(&self, db: Database) -> Result<Database, Error> {
        match File::open(self.path.clone()) {
            Ok(f) => Ok(read_db(f)),
            Err(e) if e.kind() == ErrorKind::NotFound => Ok(db),
            Err(e) => return Err(e.into()),

        }
    }
}

pub struct TempFilePersist {
    path: PathBuf,
}

impl TempFilePersist {
    pub fn new() -> Self {
        fn try_open(i: i32) -> io::Result<PathBuf> {
            let mut path = std::env::temp_dir();
            path.push(format!("rqldb-{}", i));

            OpenOptions::new()
                .read(true)
                .write(true)
                .create_new(true)
                .open(path.clone())
                .map(|_| path)
        }

        for i in 0..99 {
            match try_open(i) {
                Ok(path) => return Self{ path },
                _ => {},
            }
        }

        panic!()
    }
}

impl Persist for TempFilePersist {
    fn write(&mut self, db: &Database) -> Result<(), Error> {
        let mut file = File::options().append(true).open(self.path.clone())?;
        write_db(&mut file, &db);
        Ok(())
    }

    fn read(&self, _: Database) -> Result<Database, Error> {
        let file = File::open(self.path.clone())?;
        Ok(read_db(file))
    }
}

pub fn write_db<W: Write>(writer: &mut W, db: &Database) {
    write_schema(writer, &db.schema());
    for o in db.raw_objects() {
       write_object(writer, o.raw_tuples());
    }
}

pub fn read_db<R: Read>(reader: R) -> Database {
    let mut reader = ByteReader::new(reader);
    let schema_len = reader.peek_i32().unwrap() as usize;
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

    pub(crate) fn peek_i32(&mut self) -> io::Result<i32> {
        let buf = self.reader.fill_buf()?;
        let bytes: [u8; 4] = buf[0..4].try_into().unwrap();
        Ok(i32::from_le_bytes(bytes))
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
        db.create_table("example")
            .indexed_column("id", Type::NUMBER)
            .column("title", Type::TEXT)
            .column("content", Type::TEXT).create();

        db.create_table("other")
            .column("a", Type::TEXT)
            .column("b", Type::TEXT)
            .column("c", Type::TEXT)
            .column("d", Type::TEXT)
            .create();

        db.execute_query(&Query::tuple(&["1", "test", "stuff"]).insert_into("example")).unwrap();
        db.execute_query(&Query::tuple(&["2", "test2", "stuff"]).insert_into("example")).unwrap();

        let mut out = Vec::new();
        write_db(&mut out, &db);

        let saved_db = read_db(Cursor::new(out));
        assert!(saved_db.raw_object("example").is_some());
        let result = saved_db.execute_query(&Query::scan("example")).unwrap();
        assert_eq!(result.size(), 2);
    }

    #[test]
    fn test_write_to_file() {
        let mut db = Database::default();
        db.execute_create(&Command::create_table("example").indexed_column("id", Type::NUMBER).column("content", Type::TEXT));
        db.execute_query(&Query::tuple(&["1", "test"]).insert_into("example")).unwrap();

        let mut persist = TempFilePersist::new();
        persist.write(&mut db).unwrap();

        let saved_db = persist.read(db).unwrap();
        let result = saved_db.execute_query(&Query::scan("example")).unwrap();
        assert_eq!(result.size(), 1);
    }
}
