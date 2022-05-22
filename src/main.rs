pub mod select;
pub mod create;
pub mod schema;

use std::collections::HashMap;
use core::slice::Iter;

use select::{SelectQuery, Operator, Source};
use schema::{Schema, Type};
use create::CreateRelationCommand;

struct Database {
    schema: Schema
}

struct Tuples {
    results: Vec<HashMap<String, String>>
}

impl Database {
    pub fn new() -> Self{
        Self{schema: Schema::new()}
    }

    pub fn execute_create(&mut self, command: &CreateRelationCommand) {
        self.schema.add_relation(&command.name, &command.columns)
    }

    pub fn execute_query(&self, query: &SelectQuery) -> Result<Tuples, &str> {
        let relation = match &query.source {
            Source::TableScan(relation) => relation
        };

        let rel = self.schema.find_relation(&relation);
        if rel.is_some() {
            return Result::Ok(Tuples{results: Vec::new()});
        } else {
            return Result::Err("No such relation");
        }
    }
}

impl Tuples {
    pub fn iter(&self) -> Iter<HashMap<String, String>> {
        self.results.iter()
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
        assert_eq!(result.unwrap().iter().count(), 0);
    }
}
