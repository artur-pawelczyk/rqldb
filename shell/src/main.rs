mod table;

use rqldb::*;
use rqldb_persist::{Persist, Error as PersistError, FilePersist};
use crate::table::Table;

use rustyline::Editor;
use clap::Parser;

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
            shell.handle_command(line.trim(), false);
        }
    }

    let mut editor = Editor::<()>::new();
    loop {
        match editor.readline("query> ") {
            Ok(line) => shell.handle_command(&line, true),
            Err(e) => { println!("{:?}", e); break; }
        }
    }
}

struct Shell<'a> {
    persist: Box<dyn Persist>,
    db: Database<'a>,
}

impl<'a> Default for Shell<'a> {
    fn default() -> Self {
        Self{
            persist: Box::new(NoOpPersist),
            db: Database::default(),
        }
    }
}

impl<'a> Shell<'a> {
    fn db_file(s: &str) -> Self {
        let instance = Self{
            persist: Box::new(FilePersist::new(Path::new(s))),
            db: Database::default(),
        };

        instance.restore().unwrap()
    }

    fn handle_command(&mut self, cmd: &str, output: bool) {
        if cmd.is_empty() {
        } else if cmd == "save" {
            self.persist.write(&self.db).unwrap();
        } else if cmd.starts_with("create") {
            let command = match parse_command(cmd) {
                Ok(x) => x,
                Err(error) => { println!("{}", error); return; }
            };
            self.db.execute_create(&command);
        } else if cmd.starts_with("dump") {
            match cmd.split_ascii_whitespace().collect::<Vec<&str>>()[..] {
                ["dump", name] => println!("{}", self.db.dump(name)),
                _ => println!("Wrong command"),
            }
        } else {
            let query = match parse_query(cmd) {
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
}

struct NoOpPersist;
impl Persist for NoOpPersist {
    fn write(&mut self, _: &Database) -> Result<(), PersistError> {
        Ok(())
    }

    fn read<'a>(&self, db: Database<'a>) -> Result<Database<'a>, PersistError> {
        Ok(db)
    }
}

fn print_result(result: &QueryResults) {
    let mut table = Table::new();
    for attr in result.attributes() {
        table.add_title_cell(attr);
    }

    for res_row in result.results() {
        let mut row = table.row();
        for cell in res_row.contents() {
            row = row.cell(cell.as_string());
        }
        row.add();
    }

    println!("{}", table);
}
