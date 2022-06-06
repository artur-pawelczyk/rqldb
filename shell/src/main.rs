use relational_nosql_lib::db::{Database, QueryResults};
use relational_nosql_lib::parse::{parse_command, parse_query};
use rustyline::Editor;

fn main() {
    let mut db = Database::default();
    let mut editor = Editor::<()>::new();
    loop {
        match editor.readline("query> ") {
            Ok(line) => handle_command(&mut db, &line),
            Err(e) => { println!("{:?}", e); break; }
        }
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
    for tuple in result.results.iter() {
        let values: Vec<String> = tuple.contents.iter().map(|cell| cell.as_string()).collect();
        println!("{}", values.join(" | "));
    }
}
