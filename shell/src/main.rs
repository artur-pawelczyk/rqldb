mod table;

use rqldb::*;
use rqldb_persist::{Persist, Error as PersistError};
use crate::table::Table;

use rustyline::Editor;
use clap::Parser;

use std::io::prelude::*;
use std::fs::File;
use std::path::Path;

/// Interactive DB shell
#[derive(Parser, Debug)]
struct Args {
    /// Init file
    #[clap(long)]
    init: Option<String>,
}

fn main() {
    let args = Args::parse();
    let mut shell = Shell::default();
    if let Some(path) = args.init {
        let mut contents = String::new();
        let mut file = File::open(Path::new(&path)).unwrap();
        file.read_to_string(&mut contents).unwrap();
        for line in contents.split('\n') {
            shell.handle_command(line.trim());
        }
    }

    let mut editor = Editor::<()>::new();
    loop {
        match editor.readline("query> ") {
            Ok(line) => shell.handle_command(&line),
            Err(e) => { println!("{:?}", e); break; }
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
    fn handle_command(&mut self, cmd: &str) {
        if cmd.is_empty() {
        } else if cmd == "save" {
            self.persist.write(&self.db).unwrap();
        } else if cmd == "stat" {
            self.db.print_statistics();
        } else if cmd.starts_with("create") {
            let command = match parse_command(cmd) {
                Ok(x) => x,
                Err(error) => { println!("{}", error); return; }
            };
            self.db.execute_create(&command);
        } else {
            let query = match parse_query(cmd) {
                Ok(parsed) => parsed,
                Err(error) => { println!("{}", error); return; }
            };

            match self.db.execute_query(&query) {
                Result::Ok(response) => print_result(&response),
                Result::Err(err) => println!("{}", err),
            }
        }
    }
}

struct NoOpPersist;
impl Persist for NoOpPersist {
    fn write(&mut self, _: &Database) -> Result<(), PersistError> {
        Ok(())
    }

    fn read(self, db: Database) -> Result<Database, PersistError> {
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
            row = row.cell(&cell.as_string());
        }
        row.add();
    }

    println!("{}", table.to_string());
}
