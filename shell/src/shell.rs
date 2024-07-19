use std::{error::Error, fmt, fs::File, io, path::Path};

use rqldb::{parse_definition, parse_query, Database, QueryResults, SortOrder};
use rqldb_live_storage::LiveStorage;
use rqldb_persist::{FilePersist, Persist, Error as PersistError};

use crate::table::Table;

pub(crate) struct Shell {
    persist: Box<dyn Persist>,
    db: Database,
    result_printer: Box<dyn ResultPrinter>,
    sort: Option<(Box<str>, SortOrder)>,
    limit: Option<usize>,
}

impl Default for Shell {
    fn default() -> Self {
        Self {
            persist: Box::new(NoOpPersist),
            db: Database::default(),
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
        Ok(Self {
            db,
            ..Default::default()
        })
    }
    
    pub(crate) fn db_file(self, s: &str) -> Self {
        let instance = Self {
            persist: Box::new(FilePersist::new(Path::new(s))),
            ..self
        };

        instance.restore().unwrap()
    }

    #[cfg(test)]
    fn simple_output(self) -> Self {
        Self {
            result_printer: Box::new(SimplePrinter),
            ..self
        }
    }

    pub(crate) fn handle_input(&mut self, input: &str, output: &mut impl fmt::Write) {
        if input.is_empty() {
        } else if let Some((cmd, args)) = maybe_read_command(input) {
            if cmd == "save" {
                self.persist.write(&self.db).unwrap();
            } else if cmd == "define" {
                let command = match parse_definition(args) {
                    Ok(x) => x,
                    Err(error) => { write!(output, "{}", error).unwrap(); return; }
                };
                self.db.define(&command);
            } else if cmd == "dump" {
                if args.is_empty() {
                    self.dump_all_relations(output);
                } else {
                    self.dump_relation(args, output);
                }
            } else if cmd == "sort" {
                self.sort = if args.trim().is_empty() { None } else { read_sort_args(args) };
            } else if cmd == "limit" {
                if let Ok(limit) = args.parse() {
                    self.limit = Some(limit)
                } else {
                    self.limit = None;
                }
            } else if cmd == "output" {
                let printer: Box<dyn ResultPrinter> = match args {
                    "simple" => Box::new(SimplePrinter),
                    "table" => Box::new(TablePrinter),
                    _ => { println!("No such output type"); return; },
                };

                self.result_printer = printer;
            } else if cmd == "quit" {
                std::process::exit(0);
            }
        } else {
                let query = match parse_query(input) {
                    Ok(parsed) => parsed,
                    Err(error) => { println!("{}", error); return; }
                };

                match self.db.execute_query(&query) {
                    Result::Ok(result) => {
                        if let Some((sort_attr, ord)) = &self.sort {
                            match result.sort(sort_attr, *ord) {
                                Result::Ok(sorted) => { self.result_printer.print_result(&sorted, self.print_context(output)).unwrap() },
                                Result::Err(err) => { writeln!(output, "{err}").unwrap(); },
                            }
                        } else {
                            self.result_printer.print_result(&result, self.print_context(output)).unwrap();
                        }
                    },
                    Result::Err(err) => writeln!(output, "{}", err).unwrap(),
                }
        }
    }

    fn print_context<'a>(&self, output: &'a mut impl fmt::Write) -> PrintContext<'a> {
        let mut ctx = PrintContext::with_output(output);
        if let Some(limit) = &self.limit {
            ctx = ctx.limit(*limit);
        }

        ctx
    }

    fn restore(self) -> Result<Self, Box<dyn Error>> {
        let new_db = self.persist.read(self.db)?;
        Ok(Self {
            db: new_db,
            persist: self.persist,
            ..Default::default()
        })
    }

    fn dump_relation(&self, name: &str, output: &mut impl fmt::Write) {
        if let Err(e) = self.db.dump(name, output) {
            println!("{e}");
        }
    }

    fn dump_all_relations(&self, output: &mut impl fmt::Write) {
        if let Err(e) = self.db.dump_all(output) {
            println!("{e}");
        }
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

struct NoOpPersist;
impl Persist for NoOpPersist {
    fn write(&mut self, _: &Database) -> Result<(), PersistError> {
        Ok(())
    }

    fn read(&self, db: Database) -> Result<Database, PersistError> {
        Ok(db)
    }
}

fn maybe_read_command(input: &str) -> Option<(&str, &str)> {
    if input.chars().next() == Some('.') {
        if let Some(cmd_end) = input.char_indices().find(|(_, c)| c.is_ascii_whitespace()).map(|(i, _)| i) {
            Some((&input[1..cmd_end], &input[cmd_end..].trim()))
        } else {
            Some((&input[1..], ""))
        }
    } else {
        None
    }
}

trait ResultPrinter {
    fn print_result<'a>(&self, result: &QueryResults, ctx: PrintContext<'a>) -> Result<(), fmt::Error>;
}

struct PrintContext<'a> {
    output: &'a mut dyn fmt::Write,
    limit: Option<usize>
}

impl<'a> PrintContext<'a> {
    fn with_output(output: &'a mut dyn fmt::Write) -> Self {
        Self { output, limit: None }
    }

    fn limit(self, limit: usize) -> Self {
        Self { limit: Some(limit), ..self }
    }
}

struct TablePrinter;
impl ResultPrinter for TablePrinter {
    fn print_result<'a>(&self, result: &QueryResults, ctx: PrintContext<'a>) -> Result<(), fmt::Error> {
        let mut table = Table::new();
        for attr in result.attributes() {
            table.add_title_cell(attr.name());
        }

        for (n, res_row) in result.tuples().enumerate() {
            if ctx.limit.map(|limit| n >= limit).unwrap_or(false) {
                break;
            }

            let mut row = table.row();
            for attr in result.attributes() {
                row = row.cell(res_row.element(attr.name()).unwrap());
            }
            row.add();
        }

        writeln!(ctx.output, "{}", table)
    }
}

struct SimplePrinter;
impl ResultPrinter for SimplePrinter {
    fn print_result<'a>(&self, result: &QueryResults, ctx: PrintContext<'a>) -> Result<(), fmt::Error> {
        let attributes = result.attributes();
        for (n, tuple) in result.tuples().enumerate() {
            if ctx.limit.map(|limit| n >= limit).unwrap_or(false) {
                break;
            }

            for attr in attributes {
                writeln!(ctx.output, "{} = {}", attr.name(), tuple.element(attr.name()).unwrap())?;
            }
            writeln!(ctx.output)?;
        }

        Ok(())
    }
}

pub(crate) struct StandardOut;
impl fmt::Write for StandardOut {
    fn write_str(&mut self, s: &str) -> fmt::Result {
        print!("{}", s);
        Ok(())
    }
}

pub(crate) struct NilOut;
impl fmt::Write for NilOut {
    fn write_str(&mut self, _: &str) -> fmt::Result {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use rqldb::{Definition, Query, Type};

    use super::*;

    #[test]
    fn test_dump() {
        let mut shell = Shell::default().simple_output();
        shell.db.define(&Definition::relation("example").attribute("id", Type::NUMBER));

        let mut s = String::new();
        shell.handle_input(".dump", &mut s);
        assert_eq!(s.trim(), ".define relation example id::NUMBER");
    }

    #[test]
    fn test_print_results() {
        let mut shell = Shell::default().simple_output();
        shell.db.define(&Definition::relation("document")
                        .indexed_attribute("id", Type::NUMBER)
                        .attribute("content", Type::TEXT));
        shell.db.execute_query(&Query::tuple(&[("id", "1"), ("content", "example")])
                               .insert_into("document")).unwrap();

        let mut s = String::new();
        shell.handle_input("scan document", &mut s);
        assert_eq!(s.trim(), "
document.id = 1
document.content = example".trim());
    }

    #[test]
    fn test_sort() {
        let mut shell = Shell::default().simple_output();
        shell.db.define(&Definition::relation("document")
                        .indexed_attribute("id", Type::NUMBER)
                        .attribute("content", Type::TEXT)
                        .attribute("size", Type::NUMBER));

        shell.db.execute_query(&Query::tuple(&[("id", "1"), ("content", "example"), ("size", "123")])
                               .insert_into("document")).unwrap();
        shell.db.execute_query(&Query::tuple(&[("id", "2"), ("content", "example"), ("size", "2")])
                               .insert_into("document")).unwrap();


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
        let mut shell = Shell::default().simple_output();
        shell.db.define(&Definition::relation("document")
                        .indexed_attribute("id", Type::NUMBER)
                        .attribute("content", Type::TEXT));
        shell.db.execute_query(&Query::tuple(&[("id", "1"), ("content", "example")])
                               .insert_into("document")).unwrap();

        let mut s = String::new();
        shell.handle_input(".sort document.size", &mut NilOut);
        shell.handle_input("scan document", &mut s);

        assert_eq!(s.trim(), "Missing attribute for sort: document.size");
    }

    #[test]
    fn test_limit() {
        let mut shell = Shell::default().simple_output();
        shell.db.define(&Definition::relation("document")
                        .attribute("id", Type::NUMBER));

        for i in 0..100 {
            let id = i.to_string();
            shell.db.execute_query(&Query::tuple(&[("id", id.as_str())]).insert_into("document")).unwrap();
        }

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
