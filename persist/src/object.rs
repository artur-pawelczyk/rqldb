use std::io::{Read, Write};

use rqldb::{RawObjectView, object::TempObject};

use crate::{ByteReader, Error};

pub(crate) fn write_object<W: Write>(writer: &mut W, object: &RawObjectView) -> Result<(), Error> {
    writer.write(&size(object.count()))?;
    for tuple in object.raw_tuples() {
        // TODO: Unnecessary allocation
        let v: Vec<u8> = tuple.serialize().collect();
        writer.write(&v)?;
   }

    Ok(())
}

fn size(val: usize) -> [u8; 4] {
    (val as u32).to_le_bytes()
}

pub(crate) fn read_object<R: Read>(reader: &mut ByteReader<R>, obj: &mut TempObject) -> Result<(), Error> {
    let n_tuples = reader.read_u32()? as usize;
    for _ in 0..n_tuples {
        let n_cells = reader.read_u32()? as usize;
        let mut tuple = Vec::with_capacity(n_cells);
        for _ in 0..n_cells {
            let n_bytes = reader.read_u32()? as usize;
            let cell = reader.read_bytes(n_bytes)?;
            tuple.push(cell);
        }

        obj.push(tuple);
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use std::io::Cursor;

    use super::*;

    use rqldb::{Type, Database, Query, Command};

    #[test]
    fn test_serialize_empty_object() {
        let mut db = Database::default();
        db.execute_create(&Command::create_table("example").indexed_column("id", Type::NUMBER).column("content", Type::TEXT));

        let raw_object = db.raw_object("example").unwrap();
        let mut out = Vec::new();
        write_object(&mut out, &raw_object).unwrap();
        let mut saved_object = TempObject::with_attrs(raw_object.types());
        read_object(&mut ByteReader::new(Cursor::new(out)), &mut saved_object).unwrap();
        assert!(saved_object.is_empty());
    }

    #[test]
    fn test_serialize_object() {
        let mut db = Database::default();
        db.execute_create(&Command::create_table("example").indexed_column("id", Type::NUMBER).column("content", Type::TEXT));

        db.execute_query(&Query::tuple(&["1", "test"]).insert_into("example")).unwrap();
        db.execute_query(&Query::tuple(&["2", "test"]).insert_into("example")).unwrap();

        let raw_object = db.raw_object("example").unwrap();
        let mut out = Vec::new();
        write_object(&mut out, &raw_object).unwrap();
        let mut saved_object = TempObject::with_attrs(raw_object.types());
        read_object(&mut ByteReader::new(Cursor::new(out)), &mut saved_object).unwrap();
        

        let mut recovered_db = Database::default();
        recovered_db.execute_create(&Command::create_table("example").indexed_column("id", Type::NUMBER).column("content", Type::TEXT));
        recovered_db.recover_object(0, saved_object);
        let result = recovered_db.execute_query(&Query::scan("example")).unwrap();
        assert_eq!(result.size(), 2);
    }
}
