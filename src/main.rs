pub mod create;
pub mod db;
pub mod parse;
pub mod schema;
pub mod select;

use std::io;
use std::ops::Deref;

use crate::db::{Database, QueryResults};
use crate::parse::{parse_command, parse_query};

fn main() {
    let mut input = String::new();
    let mut db = Database::new();
    loop {
        io::stdin().read_line(&mut input).expect("Failed to read line");
        
        handle_command(&mut db, input.trim());

        if input.trim() == "quit" { break; }
        input.clear();
    }
}

fn handle_command(db: &mut Database, cmd: &str) {
    if cmd.starts_with("create") {
        let command = parse_command(cmd);
        db.execute_create(&command);
        println!("OK");
    } else {
        let query = parse_query(cmd);
        match db.execute_mut_query(&query) {
            Result::Ok(response) => print_result(&response),
            Result::Err(err) => println!("{}", err),
        }
    }
}

fn print_result(result: &QueryResults) {
    println!("{}", result.attributes.join(" | "));
    for tuple in result.results.deref() {
        let values: Vec<String> = tuple.contents.iter().map(|cell| cell.into_string()).collect();
        println!("{}", values.join(" | "));
    }
}
