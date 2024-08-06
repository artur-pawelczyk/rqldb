use std::{env::{self, Args}, fmt::Arguments, fs::{read_dir, ReadDir}, time::SystemTime};

use rqldb::{Database, Definition, Type, Query, dsl::TupleBuilder};
use rqldb_live_storage::LiveStorage;

fn main() {
    let db_dir = env::args().nth(1).expect("A path argument is required");
    read_dir(&db_dir).expect("The argument should be a directory");

    let mut db = LiveStorage::new(&db_dir).create_db().unwrap();

    if db.schema().find_relation("document").is_none() {
        db.define(&Definition::relation("document")
                  .indexed_attribute("id", Type::NUMBER)
                  .attribute("title", Type::TEXT)
                  .attribute("content", Type::TEXT));
    }


    let mut last_time = SystemTime::now();
    for i in 0..5_000_000 {
        let id = i.to_string();
        let title = format!("title {i}");
        let content = format!("content {i}");
        let query = Query::tuple(TupleBuilder::new()
                                 .inferred("id", &id)
                                 .inferred("title", &title)
                                 .inferred("content", &content)
        ).insert_into("document");
        db.execute_query(&query).unwrap();

        if i % 100_000 == 0 {
            println!("Inserted {} tuples; took {:?}", i, SystemTime::now().duration_since(last_time).unwrap());
            last_time = SystemTime::now();
        }
    }

    println!("Queriying...");
    db.execute_query(&Query::scan("document")).unwrap();
}
