mod table;

use rqldb::*;
use rqldb_persist::{Persist, Error as PersistError, FilePersist};
use rustyline::config::Configurer;
use crate::table::Table;

use rustyline::Editor;
use clap::Parser;

use core::fmt;
use std::error::Error;
use std::io::prelude::*;
use std::fs::File;
use std::path::Path;

/// Interactive DB shell
#[derive(Parser, Debug)]
struct Args {
    /// Init file
    #[clap(long)]
    init: Option<String>,

    /// When specified, execute the command and exit
    #[clap(short, long)]
    command: Option<String>,

    /// Database file. If not provided, an in-memory database is created.
    #[clap(value_name = "FILENAME")]
    db_file: Option<String>,

}

fn main() {
    let args = Args::parse();
    let mut shell = args.db_file.map(|s| Shell::db_file(&s)).unwrap_or_else(Shell::default);
    if let Some(path) = args.init {
        let mut contents = String::new();
        let mut file = File::open(Path::new(&path)).unwrap();
        file.read_to_string(&mut contents).unwrap();
        for line in contents.split('\n') {
            shell.handle_input(line.trim(), false);
        }
    }

    if let Some(command) = args.command {
        shell.handle_input(&command, true);
    } else {
        let mut editor = Editor::<()>::new();
        editor.set_auto_add_history(true);

        loop {
            match editor.readline("query> ") {
                Ok(line) => shell.handle_input(&line, true),
                Err(e) => { println!("{:?}", e); break; }
            }
        }
    }
}

struct Shell {
    persist: Box<dyn Persist>,
    db: Database,
}

impl Default for Shell {
    fn default() -> Self {
        Self{
            persist: Box::new(NoOpPersist),
            db: Database::default(),
        }
    }
}

impl Shell {
    fn db_file(s: &str) -> Self {
        let instance = Self{
            persist: Box::new(FilePersist::new(Path::new(s))),
            db: Database::default(),
        };

        instance.restore().unwrap()
    }

    fn handle_input(&mut self, input: &str, output: bool) {
        if input.is_empty() {
        } else if let Some((cmd, args)) = maybe_read_command(input) {
            if cmd == "save" {
                self.persist.write(&self.db).unwrap();
            } else if cmd == "define" {
                let command = match parse_definition(args) {
                    Ok(x) => x,
                    Err(error) => { println!("{}", error); return; }
                };
                self.db.execute_create(&command);
            } else if cmd == "dump" {
                if args.is_empty() {
                    self.dump_all_relations();
                } else {
                    self.dump_relation(args);
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
                    Result::Ok(response) => if output { print_result(&response) },
                    Result::Err(err) => println!("{}", err),
                }
        }
    }

    fn restore(self) -> Result<Self, Box<dyn Error>> {
        let new_db = self.persist.read(self.db)?;
        Ok(Self{
            db: new_db,
            persist: self.persist,
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
