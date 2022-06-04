use std::io;
use std::ops::Deref;

use relational_nosql_lib::db::{Database, QueryResults};
use relational_nosql_lib::parse::{parse_command, parse_query};

fn main() {
    let mut input = String::new();
    let mut db = Database::default();
    loop {
        io::stdin().read_line(&mut input).expect("Failed to read line");
        
        handle_command(&mut db, input.trim());

        if input.trim() == "quit" { break; }
        input.clear();
    }
}

fn handle_command(db: &mut Database, cmd: &str) {
    if cmd.starts_with("create") {
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
    println!("{}", result.attributes.join(" | "));
    for tuple in result.results.deref() {
        let values: Vec<String> = tuple.contents.iter().map(|cell| cell.into_string()).collect();
        println!("{}", values.join(" | "));
    }
}
