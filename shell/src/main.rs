mod table;

use rqldb::*;
use crate::table::Table;

use rustyline::Editor;
use clap::Parser;

use std::cmp::{max, min};
use std::io::prelude::*;
use std::fs::File;
use std::iter::zip;
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
    let mut db = Database::default();
    if let Some(path) = args.init {
        let mut contents = String::new();
        let mut file = File::open(Path::new(&path)).unwrap();
        file.read_to_string(&mut contents).unwrap();
        for line in contents.split('\n') {
            handle_command(&mut db, line.trim());
        }
    }

    let mut editor = Editor::<()>::new();
    loop {
        match editor.readline("query> ") {
            Ok(line) => handle_command(&mut db, &line),
            Err(e) => { println!("{:?}", e); break; }
        }
    }
}

fn handle_command(db: &mut Database, cmd: &str) {
    if cmd.is_empty() {
    } else if cmd == "stat" {
        db.print_statistics();
    } else if cmd.starts_with("create") {
        let command = match parse_command(cmd) {
            Ok(x) => x,
            Err(error) => { println!("{}", error); return; }
        };
        db.execute_create(&command);
    } else {
        let query = match parse_query(cmd) {
            Ok(parsed) => parsed,
            Err(error) => { println!("{}", error); return; }
        };

        match db.execute_query(&query) {
            Result::Ok(response) => print_result(&response),
            Result::Err(err) => println!("{}", err),
        }
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
