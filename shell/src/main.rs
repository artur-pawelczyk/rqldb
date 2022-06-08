use relational_nosql_lib::db::{Database, QueryResults};
use relational_nosql_lib::parse::{parse_command, parse_query};
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

        match db.execute_mut_query(&query) {
            Result::Ok(response) => print_result(&response),
            Result::Err(err) => println!("{}", err),
        }
    }
}

fn print_result(result: &QueryResults) {
    println!("{}", result.attributes().join(" | "));
    for tuple in result.results().iter() {
        let values: Vec<String> = tuple.contents().iter().map(|cell| cell.as_string()).collect();
        println!("{}", values.join(" | "));
    }
}
