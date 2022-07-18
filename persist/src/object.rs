use std::io::{Read, Write};

use crate::{ByteTuple, ByteReader};

pub(crate) fn write_object<W: Write>(writer: &mut W, tuples: &[ByteTuple]) {
    writer.write(&size(tuples.len())).unwrap();
    for tuple in tuples {
        writer.write(&size(tuple.len())).unwrap();

        for cell in tuple {
            writer.write(&size(cell.len())).unwrap();
            writer.write(&cell).unwrap();
        }
    }
}

fn size<T: TryInto<u32>>(val: T) -> [u8; 4] {
    let i: u32 = val.try_into().unwrap_or_else(|_| panic!());
    i.to_le_bytes()
}

pub(crate) fn read_object<R: Read>(reader: &mut ByteReader<R>) -> Vec<ByteTuple> {
    let n_tuples = reader.read_u32().unwrap() as usize;

    let mut tuples = Vec::with_capacity(n_tuples);
    for _ in 0..n_tuples {
        let n_cells = reader.read_u32().unwrap();

        let mut tuple = Vec::new();
        for _ in 0..n_cells {
            let n_bytes = reader.read_u32().unwrap() as usize;
            let cell = reader.read_bytes(n_bytes).unwrap();
            tuple.push(cell);
        }

        tuples.push(tuple);
    }

    tuples
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
        write_object(&mut out, raw_object.raw_tuples());
        let saved_object = read_object(&mut ByteReader::new(Cursor::new(out)));
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
        let saved_object = read_object(&mut ByteReader::new(Cursor::new(out)));
        assert_eq!(raw_object.raw_tuples(), &saved_object);
    }
}
