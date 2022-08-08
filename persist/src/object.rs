use std::io::{Read, Write};

use crate::{ByteTuple, ByteReader, Error};

pub(crate) fn write_object<W: Write>(writer: &mut W, tuples: &[ByteTuple]) -> Result<(), Error> {
    writer.write(&size(tuples.iter().count()))?;
    for tuple in tuples.iter() {
        writer.write(&size(tuple.len()))?;

        for cell in tuple {
            writer.write(&size(cell.len()))?;
            writer.write(&cell)?;
        }
    }

    Ok(())
}

fn size(val: usize) -> [u8; 4] {
    (val as u32).to_le_bytes()
}

pub(crate) fn read_object<R: Read>(reader: &mut ByteReader<R>) -> Result<Vec<ByteTuple>, Error> {
    let n_tuples = reader.read_u32()? as usize;

    let mut tuples = Vec::with_capacity(n_tuples);
    for _ in 0..n_tuples {
        let n_cells = reader.read_u32()? as usize;

        let mut tuple = Vec::with_capacity(n_cells);
        for _ in 0..n_cells {
            let n_bytes = reader.read_u32()? as usize;
            let cell = reader.read_bytes(n_bytes)?;
            tuple.push(cell);
        }

        tuples.push(tuple);
    }

    Ok(tuples)
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
        write_object(&mut out, raw_object.raw_tuples()).unwrap();
        let saved_object = read_object(&mut ByteReader::new(Cursor::new(out))).unwrap();
        assert_eq!(raw_object.raw_tuples().iter().cloned().collect::<Vec<ByteTuple>>(), saved_object);
    }

    #[test]
    fn test_serialize_object() {
        let mut db = Database::default();
        db.execute_create(&Command::create_table("example").indexed_column("id", Type::NUMBER).column("content", Type::TEXT));

        db.execute_query(&Query::tuple(&["1", "test"]).insert_into("example")).unwrap();
        db.execute_query(&Query::tuple(&["2", "test"]).insert_into("example")).unwrap();

        let raw_object = db.raw_object("example").unwrap();
        let mut out = Vec::new();
        write_object(&mut out, raw_object.raw_tuples()).unwrap();
        let saved_object = read_object(&mut ByteReader::new(Cursor::new(out))).unwrap();
        assert_eq!(raw_object.raw_tuples().iter().cloned().collect::<Vec<ByteTuple>>(), saved_object);
    }
}
