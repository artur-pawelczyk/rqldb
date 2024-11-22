use std::{cell::{Ref, RefCell}, error::Error, fmt};

use crate::{parse_definition, parse_delete, parse_insert, parse_query, Database, QueryResults};

pub trait OutputHandler {
    fn output_result(&mut self, _: QueryResults) -> Result<(), Box<dyn Error>>;
    fn custom_command(&mut self, db: &Database, cmd: &str);
}

pub struct NoopOutputHandler;
impl OutputHandler for NoopOutputHandler {
    fn output_result(&mut self, _: QueryResults) -> Result<(), Box<dyn Error>> {
        Ok(())
    }

    fn custom_command(&mut self, _: &Database, _: &str) {
    }
}

#[derive(Default)]
pub struct Interpreter {
    db: RefCell<Database>,
}

impl Interpreter {
    pub fn with_database(db: Database) -> Self {
        Self {
            db: RefCell::new(db),
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
                output.custom_command(&self.db.borrow(), s);
                Ok(())
            },
            Command::Query(query) => {
                let query = parse_query(query)?;
                let result = self.db.borrow().execute_query(&query)?;
                output.output_result(result)?;
                Ok(())
            },
            _ => Ok(())
        }
    }

    // TODO: Return something useful for insert and delete, not an error
    pub fn run_query(&self, query: &str) -> Result<QueryResults, Box<dyn Error>> {
        let mut handler = SimpleOutputHandler::new();
        self.handle_line(query, &mut handler)?;
        Ok(handler.0?)
    }

    pub fn database(&self) -> Ref<Database> {
        self.db.borrow()
    }

    fn read_command<'a>(&self, input: &'a str) -> Command<'a> {
        if input.is_empty() || input.chars().next() == Some('#') {
            Command::Empty
        } else if input.chars().next() == Some('.') {
            let cmd_end = input.char_indices()
                .find(|(_, c)| c.is_ascii_whitespace())
                .map(|(i, _)| i)
                .unwrap_or(input.len());

            match &input[1..cmd_end] {
                "define" => Command::Define(&input[cmd_end..].trim()),
                "insert" => Command::Insert(&input[cmd_end..].trim()),
                "delete" => Command::Delete(&input[cmd_end..].trim()),
                _ => Command::Custom(&input[1..]),
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

struct SimpleOutputHandler(Result<QueryResults, SimpleHandlerError>);

impl SimpleOutputHandler {
    fn new() -> Self {
        Self(Err(SimpleHandlerError::NoResult))
    }
}

impl OutputHandler for SimpleOutputHandler {
    fn output_result(&mut self, res: QueryResults) -> Result<(), Box<dyn Error>> {
        self.0 = Ok(res);
        Ok(())
    }

    fn custom_command(&mut self, _: &Database, cmd: &str) {
        self.0 = Err(SimpleHandlerError::CustomCommand(Box::from(cmd)));
    }
}

#[derive(Debug)]
enum SimpleHandlerError {
    NoResult,
    CustomCommand(Box<str>),
}

impl Error for SimpleHandlerError {}

impl fmt::Display for SimpleHandlerError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::NoResult => write!(f, "No query was executed"),
            Self::CustomCommand(c) => write!(f, "Unrecognized custom command: {c}"),
        }
    }
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
        fn output_result<'a>(&mut self, result: QueryResults) -> Result<(), Box<dyn Error>> {
            self.results.push(result);
            Ok(())
        }

        fn custom_command(&mut self, _: &Database, cmd: &str) {
            self.custom_commands.push(String::from(cmd));
        }
    }

    #[test]
    fn test_empty_command() -> Result<(), Box<dyn Error>> {
        let mut handler = MockOutputHandler::default();
        let interpreter = Interpreter::default();

        interpreter.handle_line("", &mut handler)?;

        assert!(handler.custom_commands.is_empty());
        assert!(handler.results.is_empty());

        Ok(())
    }

    #[test]
    fn test_custom_command() -> Result<(), Box<dyn Error>> {
        let mut handler = MockOutputHandler::default();
        let interpreter = Interpreter::default();
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

    #[test]
    fn test_ignore_comment() -> Result<(), Box<dyn Error>> {
        let interpreter = Interpreter::default();
        let mut handler = MockOutputHandler::default();

        interpreter.handle_line("# this is a comment", &mut handler)?;

        assert!(handler.custom_commands.is_empty());
        assert!(handler.results.is_empty());

        Ok(())
    }
}
