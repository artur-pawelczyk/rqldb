use std::{cell::RefCell, collections::HashSet, error::Error};

use crate::{parse_definition, parse_delete, parse_insert, parse_query, Database, QueryResults};

pub trait OutputHandler {
    fn output_result<'a>(&mut self, _: QueryResults);
    fn custom_command(&mut self, cmd: &str);
}

#[derive(Default)]
pub struct Interpreter {
    custom_commands: HashSet<String>,
    db: RefCell<Database>,
}

impl Interpreter {
    pub fn with_database(db: Database) -> Self {
        Self {
            db: RefCell::new(db),
            ..Default::default()
        }
    }

    pub fn custom_commands(self, commands: &[impl ToString]) -> Self {
        Self {
            custom_commands: commands.iter().map(ToString::to_string).collect(),
            ..self
        }
    }

    pub fn handle_line(&self, input: &str, output: &mut impl OutputHandler) -> Result<(), Box<dyn Error>> {
        match self.read_command(input) {
            Command::Define(args) => {
                let command = parse_definition(args)?;
                self.db.borrow_mut().define(&command)?;
                Ok(())
            },
            Command::Insert(query) => {
                let insert = parse_insert(query)?;
                self.db.borrow().insert(&insert)?;
                Ok(())
            },
            Command::Delete(query) => {
                let delete = parse_delete(query)?;
                self.db.borrow().delete(&delete)?;
                Ok(())
            },
            Command::Custom(s) => {
                output.custom_command(s);
                Ok(())
            },
            Command::Query(query) => {
                let query = parse_query(query)?;
                let result = self.db.borrow().execute_query(&query)?;
                output.output_result(result);
                Ok(())
            },
            _ => Ok(())
        }
    }

    fn read_command<'a>(&self, input: &'a str) -> Command<'a> {
        if input.chars().next() == Some('.') {
            if let Some(cmd_end) = input.char_indices().find(|(_, c)| c.is_ascii_whitespace()).map(|(i, _)| i) {
                match &input[1..cmd_end] {
                    "define" => Command::Define(&input[cmd_end..].trim()),
                    "insert" => Command::Insert(&input[cmd_end..].trim()),
                    "delete" => Command::Delete(&input[cmd_end..].trim()),
                    cmd if self.custom_commands.contains(cmd) => Command::Custom(&input[1..]),
                    _ => Command::Query(input),
                }
            } else {
                Command::Query(input)
            }
        } else {
            Command::Query(input)
        }
    }

}

enum Command<'a> {
    Define(&'a str),
    Insert(&'a str),
    Delete(&'a str),
    Custom(&'a str),
    Query(&'a str),
    Empty,
}


#[cfg(test)]
mod tests {
    use crate::test::fixture::{self, Dataset};

    use super::*;

    #[derive(Default)]
    struct MockOutputHandler {
        results: Vec<QueryResults>,
        custom_commands: Vec<String>,
    }

    impl OutputHandler for MockOutputHandler {
        fn output_result<'a>(&mut self, result: QueryResults) {
            self.results.push(result);
        }

        fn custom_command(&mut self, cmd: &str) {
            self.custom_commands.push(String::from(cmd));
        }
    }

    #[test]
    fn test_custom_command() -> Result<(), Box<dyn Error>> {
        let mut handler = MockOutputHandler::default();
        let interpreter = Interpreter::default().custom_commands(&["something"]);
        interpreter.handle_line(".something arg", &mut handler)?;

        assert_eq!(handler.custom_commands[0], "something arg");

        Ok(())
    }

    #[test]
    fn test_run_query() -> Result<(), Box<dyn Error>> {
        let db = Dataset::default()
            .add(fixture::Document::empty())
            .generate(Database::default());
        let interpreter = Interpreter::with_database(db);
        let mut handler = MockOutputHandler::default();

        interpreter.handle_line("scan document", &mut handler)?;

        assert_eq!(handler.results.len(), 1);

        Ok(())
    }
}
