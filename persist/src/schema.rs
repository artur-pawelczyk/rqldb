use std::io::{Read, Write};

use bson::{Array, Document, Bson};
use rqldb::{schema::Schema, Type};

use crate::Error;

pub(crate) fn write_schema<W: Write>(writer: &mut W, schema: &Schema) -> Result<(), Error> {
    let mut relations = Array::new();
    for rel in &schema.relations {
        let mut rel_doc = Document::new();
        rel_doc.insert("id", rel.id as i64);
        rel_doc.insert("name", rel.name.as_str());

        let mut col_arr = Array::new();
        for col in rel.columns() {
            let mut col_doc = Document::new();
            col_doc.insert("name", col.name());
            col_doc.insert("kind", col.kind().to_string());
            col_doc.insert("indexed", col.indexed());
            col_arr.push(Bson::Document(col_doc));
        }

        rel_doc.insert("columns", col_arr);

        relations.push(Bson::Document(rel_doc));
    }

    let mut doc = Document::new();
    doc.insert("relations", relations);

    doc.to_writer(writer)?;
    Ok(())
}

pub(crate) fn read_schema<R: Read>(reader: R) -> Result<Vec<Table>, Error> {
    let doc = Document::from_reader(reader)?;
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

            columns.push(Column{ name: name.to_string(), kind, indexed });
        }

        tables.push(Table{ id, name, columns });
    }

    Ok(tables)
}

pub(crate) struct Table {
    pub(crate) id: usize,
    pub(crate) name: String,
    pub(crate) columns: Vec<Column>,
}

pub(crate) struct Column {
    pub(crate) name: String,
    pub(crate) kind: Type,
    pub(crate) indexed: bool,
}

#[cfg(test)]
mod tests {
    use std::{io::Cursor, iter::zip};

    use rqldb::{Type, Database, Command};

    use super::*;

    #[test]
    fn test_serialize_schema() {
        let mut db = Database::default();
        db.execute_create(&Command::create_table("example")
                          .indexed_column("id", Type::NUMBER)
                          .column("content", Type::TEXT));

        let mut out = Vec::new();
        write_schema(&mut out, db.schema()).unwrap();

        let saved_schema = read_schema(Cursor::new(out)).unwrap();

        for (rel, saved_rel) in zip(&db.schema().relations, saved_schema) {
            assert_eq!(rel.id, saved_rel.id);
            assert_eq!(rel.name, saved_rel.name);
            assert_eq!(rel.columns().count(), saved_rel.columns.len());
        }
    }


}
