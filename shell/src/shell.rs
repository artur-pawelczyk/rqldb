use std::{cell::Ref, error::Error, fmt, io};

use rqldb::{interpret::{Interpreter, OutputHandler}, Database, SortOrder};
use rqldb_live_storage::LiveStorage;

use crate::print::{PrintContext, ResultPrinter, SimplePrinter, TablePrinter};

pub(crate) struct Shell {
    interpreter: Interpreter,
    result_printer: Box<dyn ResultPrinter>,
    sort: Option<(Box<str>, SortOrder)>,
    limit: Option<usize>,
}

impl Default for Shell {
    fn default() -> Self {
        Self {
            interpreter: Default::default(),
            result_printer: Box::new(TablePrinter),
            sort: None,
            limit: None,
        }
    }
}

impl Shell {
    pub(crate) fn with_db_dir(s: &str) -> io::Result<Self> {
        let storage = LiveStorage::new(s);
        let db = storage.create_db()?;
        Ok(Self::with_database(db))
    }

    pub(crate) fn with_database(db: Database) -> Self {
        Self {
            interpreter: Interpreter::with_database(db),
            ..Default::default()
        }
    }
    
    #[cfg(test)]
    fn simple_output(self) -> Self {
        use crate::print::SimplePrinter;

        Self {
            result_printer: Box::new(SimplePrinter),
            ..self
        }
    }

    pub(crate) fn handle_input(&mut self, input: &str, output: &mut impl fmt::Write) {
        let mut handler = ShellOutputHandler {
            printer: self.result_printer.as_ref(),
            print_ctx: self.print_context(output),
            sort: &self.sort,
            last_command: String::new(),
        };

        match self.interpreter.handle_line(input, &mut handler) {
            Err(e) => {
                writeln!(output, "{e}").unwrap();
            } _ => if !handler.last_command.is_empty() {
                if let Err(e) = self.handle_custom_command(&handler.last_command, output) {
                    writeln!(output, "{e}").unwrap();
                }
            }
        }
    }

    fn handle_custom_command(&mut self, input: &str, output: &mut impl fmt::Write) -> Result<(), Box<dyn Error>> {
        match maybe_read_command(input) {
            Some(("dump", "")) => {
                self.database().dump_all(output)?;
                Ok(())
            },
            Some(("dump", relation)) => {
                self.database().dump(relation, output)?;
                Ok(())
            },
            Some(("sort", sort)) => {
                self.sort = if sort.trim().is_empty() { None } else { read_sort_args(sort) };
                Ok(())
            },
            Some(("limit", limit)) => {
                self.limit = limit.parse().ok();
                Ok(())
            },
            Some(("output", "simple")) => {
                self.result_printer = Box::new(SimplePrinter);
                Ok(())
            },
            Some(("output", "table")) => {
                self.result_printer = Box::new(TablePrinter);
                Ok(())
            },
            Some(("output", printer)) => {
                Err(ShellError(format!("Printer {printer} not supported")))?;
                Ok(())
            },
            Some(("quit", _)) => {
                std::process::exit(0);
            },
            _ => {
                Err(ShellError(format!("Command not recognized")))?;
                Ok(())
            },
        }
    }

    fn database(&self) -> Ref<Database> {
        self.interpreter.database()
    }

    fn print_context<'a>(&self, output: &'a mut impl fmt::Write) -> PrintContext<'a> {
        let mut ctx = PrintContext::with_output(output);
        if let Some(limit) = &self.limit {
            ctx = ctx.limit(*limit);
        }

        ctx
    }
}

struct ShellOutputHandler<'a> {
    printer: &'a dyn ResultPrinter,
    print_ctx: PrintContext<'a>,
    sort: &'a Option<(Box<str>, SortOrder)>,
    last_command: String,
}

impl<'a> OutputHandler for ShellOutputHandler<'a> {
    fn output_result(&mut self, result: rqldb::QueryResults) -> Result<(), Box<dyn Error>> {
        if let Some((sort_attr, ord)) = self.sort {
            let sorted = result.sort(sort_attr, *ord)?;
            self.printer.print_result(&sorted, &mut self.print_ctx)?;
        } else {
            self.printer.print_result(&result, &mut self.print_ctx)?;
        }

        Ok(())
    }

    fn custom_command(&mut self, _: &Database, cmd: &str) {
        self.last_command.clear();
        self.last_command.push_str(cmd.trim());
    }
}

fn read_sort_args(args: &str) -> Option<(Box<str>, SortOrder)> {
    let mut args = args.split_ascii_whitespace();
    if let Some(attr) = args.next() {
        if let Some(ord) = args.next() {
            let ord = if ord == "desc" { SortOrder::LargestFirst } else { SortOrder::SmallestFirst };
            Some((Box::from(attr), ord))
        } else {
            Some((Box::from(attr), SortOrder::default()))
        }
    } else {
        None
    }
}

fn maybe_read_command(input: &str) -> Option<(&str, &str)> {
    if !input.is_empty() {
        if let Some(cmd_end) = input.char_indices().find(|(_, c)| c.is_ascii_whitespace()).map(|(i, _)| i) {
            Some((&input[..cmd_end], &input[cmd_end..].trim()))
        } else {
            Some((input, ""))
        }
    } else {
        None
    }
}

#[derive(Debug)]
pub(crate) struct ShellError(String);

impl Error for ShellError {}

impl fmt::Display for ShellError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}
    

#[cfg(test)]
mod tests {
    use rqldb::{Database, Definition, Query, Type};

    use crate::print::NilOut;

    use super::*;

    #[test]
    fn test_dump() {
        let mut db = Database::default();
        db.define(&Definition::relation("example").attribute("id", Type::NUMBER)).unwrap();
        let mut shell = Shell::with_database(db).simple_output();

        let mut s = String::new();
        shell.handle_input(".dump", &mut s);
        assert_eq!(s.trim(), ".define relation example id::NUMBER");
    }

    #[test]
    fn test_print_results() {
        let mut db = Database::default();
        db.define(&Definition::relation("document")
                        .indexed_attribute("id", Type::NUMBER)
                        .attribute("content", Type::TEXT)).unwrap();
        db.insert(&Query::tuple(&[("id", "1"), ("content", "example")])
                  .insert_into("document")).unwrap();
        let mut shell = Shell::with_database(db).simple_output();

        let mut s = String::new();
        shell.handle_input("scan document", &mut s);
        assert_eq!(s.trim(), "
document.id = 1
document.content = example".trim());
    }

    #[test]
    fn test_sort() {
        let mut db = Database::default();
        db.define(&Definition::relation("document")
                        .indexed_attribute("id", Type::NUMBER)
                        .attribute("content", Type::TEXT)
                        .attribute("size", Type::NUMBER)).unwrap();

        db.insert(&Query::tuple(&[("id", "1"), ("content", "example"), ("size", "123")])
                               .insert_into("document")).unwrap();
        db.insert(&Query::tuple(&[("id", "2"), ("content", "example"), ("size", "2")])
                               .insert_into("document")).unwrap();

        let mut shell = Shell::with_database(db).simple_output();

        let mut s = String::new();
        shell.handle_input(".sort document.size", &mut NilOut);
        shell.handle_input("scan document", &mut s);
        assert_eq!(s.trim(), "
document.id = 2
document.content = example
document.size = 2

document.id = 1
document.content = example
document.size = 123".trim());

        let mut s = String::new();
        shell.handle_input(".sort document.size desc", &mut NilOut);
        shell.handle_input("scan document", &mut s);
        assert_eq!(s.trim(), "
document.id = 1
document.content = example
document.size = 123

document.id = 2
document.content = example
document.size = 2".trim());
    }

    #[test]
    fn test_sort_missing_attribute() {
        let mut db = Database::default();
        db.define(&Definition::relation("document")
                        .indexed_attribute("id", Type::NUMBER)
                        .attribute("content", Type::TEXT)).unwrap();
        db.insert(&Query::tuple(&[("id", "1"), ("content", "example")])
                  .insert_into("document")).unwrap();

        let mut shell = Shell::with_database(db).simple_output();

        let mut s = String::new();
        shell.handle_input(".sort document.size", &mut NilOut);
        shell.handle_input("scan document", &mut s);

        assert_eq!(s.trim(), "Missing attribute for sort: document.size");
    }

    #[test]
    fn test_limit() {
        let mut db = Database::default();
        db.define(&Definition::relation("document")
                        .attribute("id", Type::NUMBER)).unwrap();

        for i in 0..100 {
            let id = i.to_string();
            db.insert(&Query::tuple(&[("id", id.as_str())]).insert_into("document")).unwrap();
        }

        let mut shell = Shell::with_database(db).simple_output();

        let mut s = String::new();
        shell.handle_input(".limit 10", &mut NilOut);
        shell.handle_input("scan document", &mut s);
        assert_eq!(s.lines().count(), 20);

        let mut s = String::new();
        shell.handle_input(".limit", &mut NilOut);
        shell.handle_input("scan document", &mut s);
        assert_eq!(s.lines().count(), 200);
    }
}
