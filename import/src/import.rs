use std::borrow::Cow;
use std::collections::BTreeMap;
use std::collections::HashMap;
use std::io;
use std::fmt;

use csv::{Reader, StringRecord};
use rqldb::dsl::Insert;
use rqldb::dsl::TupleAttr;

pub struct Import<'a> {
    target: &'a str,
    mapping: HashMap<&'a str, &'a str>,
}

impl<'a> Import<'a> {
    pub fn into_table(target: &'a str) -> Self {
        Self { target, mapping: HashMap::new() }
    }

    pub fn column(mut self, from: &'a str, to: &'a str) -> Self {
        self.mapping.insert(from, to);
        self
    }

    pub fn import_string(&self, s: &str, consumer: impl FnMut(Insert<Vec<TupleAttr<'_>>>) -> ()) -> Result<(), Error> {
        self.import(Reader::from_reader(s.as_bytes()), consumer)
    }

    pub fn import<R: io::Read>(&self, mut reader: Reader<R>, mut consumer: impl FnMut(Insert<Vec<TupleAttr<'_>>>) -> ()) -> Result<(), Error> {
        let header = reader.headers()?;
        let name_to_position: BTreeMap<&str, usize> = self.mapping.iter()
            .map(|(from, to)| (*to, Self::find_col(header, from).unwrap()))
            .collect();
        for result in reader.records() {
            let record = result?;
            let mut tuple = Vec::new();
            for (name, pos) in name_to_position.iter() {
                tuple.push(TupleAttr { name, value: Cow::Borrowed(record.get(*pos).unwrap()), kind: None });
            }

            consumer(Insert::insert_into(self.target).tuple(tuple));
        }
        
        Ok(())
    }

    fn find_col(header: &StringRecord, expected: &'a str) -> Result<usize, Error> {
        if let Some(pos) = header.iter().position(|name| name == expected) {
            Ok(pos)
        } else {
            Err(Error(format!("Could not find column: {}", expected)))
        }
    }
}

#[derive(Debug)]
pub struct Error(String);

impl std::error::Error for Error {
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "Error importing CSV: {}", self.0)
    }
}

impl From<csv::Error> for Error {
    fn from(e: csv::Error) -> Self {
        Self(format!("{}", e))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn import_csv() {
        let csv = "id,garbage,content
1,nothing,first
2,nothing,second";
        let reader = Reader::from_reader(csv.as_bytes());

        let import = Import::into_table("document")
            .column("id", "id")
            .column("content", "content");
        let mut queries = Vec::new();
        import.import(reader, |query| {
            queries.push(format!(".insert {query}"));
        }).unwrap();
        
        assert_eq!(queries.len(), 2);
        assert_eq!(queries[0].to_string(), ".insert document content = first id = 1");
        assert_eq!(queries[1].to_string(), ".insert document content = second id = 2")
    }
}
