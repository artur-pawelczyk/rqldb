use std::{error::Error, fmt, path::Path};

use rqldb::{parse_definition, parse_query, Database, QueryResults};
use rqldb_persist::{FilePersist, Persist, Error as PersistError};

use crate::table::Table;

pub(crate) struct Shell {
    persist: Box<dyn Persist>,
    db: Database,
    last_result: Option<QueryResults>,
}

impl Default for Shell {
    fn default() -> Self {
        Self {
            persist: Box::new(NoOpPersist),
            db: Database::default(),
            last_result: None,
        }
    }
}

impl Shell {
    pub(crate) fn with_db_file(s: &str) -> Self {
        let instance = Self {
            persist: Box::new(FilePersist::new(Path::new(s))),
            db: Database::default(),
            last_result: None,
        };

        instance.restore().unwrap()
    }

    pub(crate) fn handle_input(&mut self, input: &str, output: bool) {
        if input.is_empty() {
        } else if let Some((cmd, args)) = maybe_read_command(input) {
            if cmd == "save" {
                self.persist.write(&self.db).unwrap();
            } else if cmd == "define" {
                let command = match parse_definition(args) {
                    Ok(x) => x,
                    Err(error) => { println!("{}", error); return; }
                };
                self.db.define(&command);
            } else if cmd == "dump" {
                if args.is_empty() {
                    self.dump_all_relations();
                } else {
                    self.dump_relation(args);
                }
            } else if cmd == "print" {
                if let Some(result) = &self.last_result {
                    print_result(&result);
                }
            } else if cmd == "quit" {
                std::process::exit(0);
            }
        } else {
                let query = match parse_query(input) {
                    Ok(parsed) => parsed,
                    Err(error) => { println!("{}", error); return; }
                };

                match self.db.execute_query(&query) {
                    Result::Ok(response) => if output {
                        print_result(&response);
                        self.last_result.replace(response);
                    },
                    Result::Err(err) => println!("{}", err),
                }
        }
    }

    fn restore(self) -> Result<Self, Box<dyn Error>> {
        let new_db = self.persist.read(self.db)?;
        Ok(Self {
            db: new_db,
            persist: self.persist,
            last_result: None,
        })
    }

    fn dump_relation(&self, name: &str) {
        if let Err(e) = self.db.dump(name, &mut StandardOut) {
            println!("{e}");
        }
    }

    fn dump_all_relations(&self) {
        if let Err(e) = self.db.dump_all(&mut StandardOut) {
            println!("{e}");
        }
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

fn print_result(result: &QueryResults) {
    let mut table = Table::new();
    for attr in result.attributes() {
        table.add_title_cell(attr.name());
    }

    for res_row in result.tuples() {
        let mut row = table.row();
        for attr in result.attributes() {
            row = row.cell(res_row.element(attr.name()).unwrap());
        }
        row.add();
    }

    println!("{}", table);
}


struct StandardOut;
impl fmt::Write for StandardOut {
    fn write_str(&mut self, s: &str) -> fmt::Result {
        println!("{}", s);
        Ok(())
    }
}
