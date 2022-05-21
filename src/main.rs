pub mod select;
pub mod create;
pub mod schema;

use select::{SelectQuery, Operator};
use schema::{Schema, Type};
use create::CreateRelationCommand;

struct Database {
    schema: Schema
}

impl Database {
    pub fn new() -> Self{
        Self{schema: Schema::new()}
    }

    pub fn execute_create(&mut self, command: &CreateRelationCommand) {
        self.schema.add_relation(&command.name, &command.columns)
    }
}

fn main() {
    let command = create::CreateRelationCommand::with_name("document")
        .column("id", Type::NUMBER)
        .column("content", Type::TEXT);

    let mut db = Database::new();
    db.execute_create(&command);
}
