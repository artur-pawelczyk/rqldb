pub mod select;
pub mod create;

use select::{SelectQuery, Operator};
use create::{CreateRelationCommand, Type};

struct Database {
}

impl Database {
    pub fn new() -> Self{
        Self{}
    }

    pub fn execute_create(&self, command: &CreateRelationCommand) {
    }
}

fn main() {
    let command = CreateRelationCommand::with_name("document")
        .column("id", Type::NUMBER)
        .column("content", Type::TEXT);

    let db = Database::new();
    db.execute_create(&command);
}
