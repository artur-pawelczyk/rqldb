pub mod select;
pub mod create;
pub mod schema;

use std::collections::HashMap;
use std::rc::Rc;
use std::ops::Deref;

use select::{SelectQuery, Operator, Source};
use schema::{Schema, Type};
use create::CreateRelationCommand;

struct Database {
    schema: Schema
}

struct Tuples {
    results: Vec<Tuple>,
    attributes: Rc<Vec<String>>
}

type Tuple = Vec<Vec<u8>>;

impl Database {
    pub fn new() -> Self{
        Self{schema: Schema::new()}
    }

    pub fn execute_create(&mut self, command: &CreateRelationCommand) {
        self.schema.add_relation(&command.name, &command.columns)
    }

    pub fn execute_query(&self, query: &SelectQuery) -> Result<Tuples, &str> {
        let tuples = match &query.source {
            Source::TableScan(name) => {
                let rel = self.schema.find_relation(&name).ok_or("No such relation")?;
                Tuples::empty(rel.columns.iter().map(|col| col.name.clone()).collect())
            },
            Source::Tuple(values) => {
                Tuples::single_unnamed(values.iter().map(|x| x.to_owned().into_bytes()).collect())
            }
        };

        Result::Ok(tuples)
    }
}

impl Tuples {
    pub fn empty(attributes: Vec<String>) -> Self {
        Self{attributes: Rc::new(attributes), results: Vec::new()}
    }

    pub fn single_unnamed(values: Tuple) -> Self {
        Self{attributes: Rc::new(Vec::new()), results: vec![values]}
    }

    pub fn size(&self) -> u32 {
        self.results.len() as u32
    }

    pub fn attributes(&self) -> Rc<Vec<String>> {
        Rc::clone(&self.attributes)
    }
}

fn main() {
    let command = CreateRelationCommand::with_name("document")
        .column("id", Type::NUMBER)
        .column("content", Type::TEXT);

    let mut db = Database::new();
    db.execute_create(&command);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn query_not_existing_relation() {
        let query = SelectQuery::scan("not_real_relation").select_all();
        let db = Database::new();
        let result = db.execute_query(&query);
        assert_eq!(result.is_ok(), false);
    }

    #[test]
    fn query_empty_relation() {
        let mut db = Database::new();
        let command = CreateRelationCommand::with_name("document")
            .column("id", Type::NUMBER)
            .column("content", Type::TEXT);

        db.execute_create(&command);

        let query = SelectQuery::scan("document").select_all();
        let result = db.execute_query(&query);

        assert_eq!(result.is_ok(), true);
        let tuples = result.unwrap();
        assert_eq!(tuples.size(), 0);
        let attrs = tuples.attributes();
        assert_eq!(attrs.as_slice(), ["id".to_string(), "content".to_string()]);
    }

    #[test]
    fn insert() {
        let mut db = Database::new();

        let command = CreateRelationCommand::with_name("document")
            .column("id", Type::NUMBER)
            .column("content", Type::TEXT);
        db.execute_create(&command);

        let insert_query = SelectQuery::tuple(&["1", "something"]).insert_into("document");
        let insert_result = db.execute_query(&insert_query);
        assert_eq!(insert_result.is_ok(), true);

        let query = SelectQuery::scan("document").select_all();
        let result = db.execute_query(&query);
        assert_eq!(result.is_ok(), true);
    }
}
