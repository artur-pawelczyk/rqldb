use core::fmt;
use std::io::{self, Read, Write};

use rqldb::{schema::Schema, Type};

pub(crate) fn read_schema<R: Read>(reader: R) -> Result<Vec<Table>, Error> {
    let doc = bson::Document::from_reader(reader)?;
    let mut tables = Vec::new();

    for rel_doc in doc.get_array("relations")?.iter().flat_map(|o| o.as_document()) {
        let id = rel_doc.get_i64("id")? as usize;
        let name = rel_doc.get_str("name")?.to_string();
        let mut columns = Vec::new();
        for col in rel_doc.get_array("columns")? {
            let col_doc = col.as_document().ok_or(Error::UnexpectedValueError)?;
            let name = col_doc.get_str("name")?;
            let kind = col_doc.get_str("kind")?.parse().unwrap();
            let indexed = col_doc.get_bool("indexed")?;

            columns.push(Column { name: Box::from(name), kind, indexed });
        }

        tables.push(Table { id, name: Box::from(name), columns: Box::from(columns) });
    }


    Ok(tables)
}

pub(crate) fn write_schema<W: Write>(writer: &mut W, schema: &Schema) -> Result<(), Error> {
    let mut relations = bson::Array::new();
    for rel in &schema.relations {
        let mut rel_doc = bson::Document::new();
        rel_doc.insert("id", rel.id as i64);
        rel_doc.insert("name", rel.name.as_ref());

        let mut col_arr = bson::Array::new();
        for col in rel.attributes() {
            let mut col_doc = bson::Document::new();
            col_doc.insert("name", col.short_name());
            col_doc.insert("kind", col.kind().to_string());
            col_doc.insert("indexed", col.indexed());
            col_arr.push(bson::Bson::Document(col_doc));
        }

        rel_doc.insert("columns", col_arr);

        relations.push(bson::Bson::Document(rel_doc));
    }

    let mut doc = bson::Document::new();
    doc.insert("relations", relations);

    doc.to_writer(writer)?;
    Ok(())
}

pub(crate) struct Table {
    pub(crate) id: usize,
    pub(crate) name: Box<str>,
    pub(crate) columns: Box<[Column]>,
}

pub(crate) struct Column {
    pub(crate) name: Box<str>,
    pub(crate) kind: Type,
    pub(crate) indexed: bool,
}

#[derive(Debug)]
pub(crate) enum Error {
    IOError(io::Error),
    SerializationError(Box<dyn std::error::Error>),
    DeserializationError(Box<dyn std::error::Error>),
    UnexpectedValueError,
}

impl std::error::Error for Error {}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::IOError(e) => write!(f, "{e}"),
            Self::SerializationError(e) => write!(f, "Serialization error: {e}"),
            Self::DeserializationError(e) => write!(f, "Deserialization error: {e}"),
            Self::UnexpectedValueError => write!(f, "Unexpected value while deserializing schema"),
        }
    }
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
