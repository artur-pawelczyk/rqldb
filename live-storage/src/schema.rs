use core::fmt;
use std::io::{self, Read, Write};

use rqldb::{Type, schema::{Relation, Schema as DbSchema}};
use serde::{Deserialize, Serialize};

pub(crate) fn read_schema<R: Read>(reader: R) -> Result<Schema, Error> {
    let doc = bson::Document::from_reader(reader)?;
    let schema: Schema = bson::deserialize_from_document(doc)?;

    Ok(schema)
}

pub(crate) fn write_schema<W: Write>(writer: &mut W, schema: Schema) -> Result<(), Error> {
    let doc = bson::serialize_to_document(&schema)?;
    doc.to_writer(writer)?;

    Ok(())
}

#[derive(Serialize, Deserialize)]
pub(crate) struct Schema {
    pub(crate) tables: Vec<Table>,
}

impl From<&DbSchema> for Schema {
    fn from(value: &DbSchema) -> Self {
        Self { tables: value.relations.iter().map(Into::into).collect() }
    }
}

#[derive(Serialize, Deserialize)]
pub(crate) struct Table {
    pub(crate) id: usize,
    pub(crate) name: Box<str>,
    pub(crate) columns: Box<[Column]>,
}

impl From<&Relation> for Table {
    fn from(value: &Relation) -> Self {
        let columns = value.attributes()
            .map(|attr| Column {
                name: attr.short_name().into(),
                kind: attr.kind(),
                indexed: attr.indexed()
            }).collect();

        Self {
            id: value.id as usize,
            name: value.name().into(),
            columns
        }
    }
}

#[derive(Serialize, Deserialize)]
pub(crate) struct Column {
    pub(crate) name: Box<str>,
    pub(crate) kind: Type,
    pub(crate) indexed: bool,
}

#[derive(Debug)]
pub(crate) enum Error {
    IO(io::Error),
    Bson(Box<dyn std::error::Error>),
}

impl std::error::Error for Error {}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::IO(e) => write!(f, "{e}"),
            Self::Bson(e) => write!(f, "Bson error: {e}"),
        }
    }
}

impl From<io::Error> for Error {
    fn from(e: io::Error) -> Error {
        Error::IO(e)
    }
}

impl From<bson::error::Error> for Error {
    fn from(value: bson::error::Error) -> Self {
        Error::Bson(Box::new(value))
    }
}
