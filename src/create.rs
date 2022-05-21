use std::fmt;

use crate::schema::{Column, Type};

pub struct CreateRelationCommand {
    name: String,
    columns: Vec<Column>
}

impl CreateRelationCommand {
    pub fn with_name(name: &str) -> Self {
        CreateRelationCommand{name: name.to_string(), columns: Vec::new()}
    }

    pub fn column(mut self, name: &str, kind: Type) -> Self {
        self.columns.push(Column{name: name.to_string(), kind: kind});
        self
    }

    pub fn print(&self) -> String {
        let mut s = String::new();
        s.push_str("create_table ");
        s.push_str(&self.name);
        s.push(' ');

        for col in &self.columns {
            s.push_str(&col.print());
            s.push(' ');
        }
        s.pop();

        s
    }
}

impl fmt::Display for CreateRelationCommand {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.print())
    }
}

impl Column {
    fn print(&self) -> String {
        self.name.clone() + "::" + self.kind.print()
    }
}

impl Type {
    fn print(&self) -> &'static str {
        match self {
            Type::NUMBER => "NUMBER",
            Type::TEXT => "TEXT"
        }
    }
}

#[cfg(test)]
mod tests {
    use super::CreateRelationCommand;
    use crate::schema::Type::*;

    #[test]
    fn create_table() {
        let query = CreateRelationCommand::with_name("example")
            .column("id", NUMBER)
            .column("contents", TEXT);

        assert_eq!("create_table example id::NUMBER contents::TEXT", query.to_string());
    }

}
