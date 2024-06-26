mod object;
mod schema;

use object::{write_object, read_object};
use rqldb::Database;
use rqldb::object::TempObject;
use rqldb::schema::Schema;
use schema::{write_schema, read_schema};

use std::fs::{File, OpenOptions};
use std::io::{self, Cursor, ErrorKind};
use std::io::BufRead;
use std::io::BufReader;
use std::io::Read;
use std::io::Write;
use std::path::{PathBuf, Path};
use std::error;
use std::fmt;
use std::sync::Arc;

#[derive(Debug)]
pub enum Error {
    IOError(io::Error),
    SerializationError(Box<dyn std::error::Error>),
    DeserializationError(Box<dyn std::error::Error>),
    BsonIOError(Arc<io::Error>),
    UnexpectedValueError,
}

impl From<io::Error> for Error {
    fn from(e: io::Error) -> Error {
        Error::IOError(e)
    }
}

impl From<bson::ser::Error> for Error {
    fn from(e: bson::ser::Error) -> Self {
        Self::SerializationError(Box::new(e))
    }
}

impl From<bson::de::Error> for Error {
    fn from(e: bson::de::Error) -> Self {
        Self::DeserializationError(Box::new(e))
    }
}

impl From<bson::document::ValueAccessError> for Error {
    fn from(e: bson::document::ValueAccessError) -> Self {
        Self::DeserializationError(Box::new(e))
    }
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::IOError(e) => write!(f, "IO error: {}", e),
            Self::SerializationError(e) => write!(f, "Serialization error: {}", e),
            Self::DeserializationError(e) => write!(f, "Deserialization error: {}", e),
            Self::BsonIOError(e) => write!(f, "Bson IO error: {}", e),
            Self::UnexpectedValueError => write!(f, "Unexpected value while parsing schema"),
        }
    }
}

impl error::Error for Error {}

// TODO: Split into two traits. "read" should consume self
pub trait Persist {
    fn write(&mut self, db: &Database) -> Result<(), Error>;
    fn read<'a>(&self, db: Database) -> Result<Database, Error>;
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
        write_db(&mut file, db)?;
        Ok(())
    }

    fn read<'a>(&self, db: Database) -> Result<Database, Error> {
        match File::open(self.path.clone()) {
            Ok(f) => Ok(read_db(f).unwrap()),
            Err(e) if e.kind() == ErrorKind::NotFound => Ok(db),
            Err(e) => Err(e.into()),

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
            if let Ok(path) = try_open(i) {
                return Self{ path };
            }
        }

        panic!("Creating tempary file: no more filenames available");
    }
}

impl Default for TempFilePersist {
    fn default() -> Self {
        Self::new()
    }
}

impl Persist for TempFilePersist {
    fn write(&mut self, db: &Database) -> Result<(), Error> {
        let mut file = File::options().append(true).open(self.path.clone())?;
        write_db(&mut file, db)?;
        Ok(())
    }

    fn read(&self, _: Database) -> Result<Database, Error> {
        let file = File::open(self.path.clone())?;
        read_db(file)
    }
}

pub fn write_db<W: Write>(writer: &mut W, db: &Database) -> Result<(), Error> {
    write_schema(writer, db.schema())?;
    for o in db.raw_objects() {
       write_object(writer, &o)?
    }

    Ok(())
}

pub fn read_db<R: Read>(reader: R) -> Result<Database, Error> {
    let mut reader = ByteReader::new(reader);
    let schema_len = reader.peek_i32().unwrap() as usize;
    let schema_bytes = reader.read_bytes(schema_len).unwrap();

    let mut schema = Schema::default();
    let mut tables = read_schema(Cursor::new(schema_bytes)).unwrap();
    tables.sort_by_key(|x| x.id);
    for table in tables {
        table.columns.iter().fold(schema.create_table(&table.name), |acc, col| {
            if col.indexed {
                acc.indexed_column(&col.name, col.kind)
            } else {
                acc.column(&col.name, col.kind)
            }
        }).add();
    }

    let mut db = Database::with_schema(schema);

    let mut object_id = 0usize;
    while reader.has_some().unwrap() {
        let relation = &db.schema().relations[object_id];
        let mut object = TempObject::from_relation(relation);
        read_object(&mut reader, &mut object).unwrap();
        db.recover_object(object_id, object);
        object_id += 1;
    }

    Ok(db)
}

pub(crate) struct ByteReader<R: Read> {
    reader: BufReader<R>,
}

impl<R: Read> ByteReader<R> {
    pub(crate) fn new(reader: R) -> Self {
        Self{ reader: BufReader::new(reader) }
    }

    pub(crate) fn peek_i32(&mut self) -> io::Result<i32> {
        const LEN: usize = i32::BITS as usize / 8;
        let buf = self.reader.fill_buf()?;
        let bytes: [u8; LEN] = buf[0..LEN].try_into().unwrap();
        Ok(i32::from_le_bytes(bytes))
    }

    pub(crate) fn read_u32(&mut self) -> io::Result<i32> {
        const LEN: usize = u32::BITS as usize / 8;
        let mut buf = [0u8; LEN];
        self.reader.read_exact(&mut buf).unwrap();
        Ok(i32::from_le_bytes(buf))
    }

    pub(crate) fn read_bytes(&mut self, size: usize) -> io::Result<Vec<u8>> {
        let mut out: Vec<u8> = vec![0; size];
        self.reader.read_exact(&mut out).unwrap();

        Ok(out)
    }

    pub(crate) fn has_some(&mut self) -> io::Result<bool> {
        Ok(!self.reader.fill_buf().unwrap().is_empty())
    }
}

#[cfg(test)]
mod tests {
    use rqldb::{Definition, Type, Query, Operator};

    use super::*;

    #[test]
    fn test_serialize_db() {
        let mut db = Database::default();
        db.define(&Definition::relation("example")
                          .indexed_attribute("id", Type::NUMBER)
                          .attribute("title", Type::TEXT)
                          .attribute("content", Type::TEXT));

        db.define(&Definition::relation("other")
                          .attribute("a", Type::TEXT)
                          .attribute("b", Type::TEXT)
                          .attribute("c", Type::TEXT)
                          .attribute("d", Type::TEXT));

        db.execute_query(&Query::tuple(&[("id", "1"), ("title", "test1"), ("content", "stuff")]).insert_into("example")).unwrap();
        db.execute_query(&Query::tuple(&[("id", "2"), ("title", "test2"), ("content", "stuff")]).insert_into("example")).unwrap();
        db.execute_query(&Query::tuple(&[("id", "3"), ("title", "test3"), ("content", "stuff")]).insert_into("example")).unwrap();
        db.execute_query(&Query::scan("example").filter("example.id", Operator::EQ, "2").delete()).unwrap();

        let mut out = Vec::new();
        write_db(&mut out, &db).unwrap();

        let saved_db = read_db(Cursor::new(out)).unwrap();
        assert!(saved_db.raw_object("example").is_some());
        let result = saved_db.execute_query(&Query::scan("example")).unwrap();
        assert_eq!(result.tuples().count(), 2);
    }

    #[test]
    fn test_write_to_file() {
        let mut db = Database::default();
        db.define(&Definition::relation("example").indexed_attribute("id", Type::NUMBER).attribute("content", Type::TEXT));
        db.execute_query(&Query::tuple(&[("id", "1"), ("content", "test")]).insert_into("example")).unwrap();

        let mut persist = TempFilePersist::new();
        persist.write(&db).unwrap();

        let saved_db = persist.read(db).unwrap();
        let result = saved_db.execute_query(&Query::scan("example")).unwrap();
        assert_eq!(result.tuples().count(), 1);
    }
}
