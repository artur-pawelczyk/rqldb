use std::{env, fs::read_dir, time::SystemTime};

use rqldb::{dsl::TupleBuilder, Database, Definition, Query, Type};
use rqldb_live_storage::LiveStorage;

fn main() {
    let mut db = db_dir_argument()
        .map(|p| LiveStorage::new(&p).create_db().unwrap())
        .unwrap_or(Database::default());

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

fn db_dir_argument() -> Option<String> {
    let path = env::args().nth(1)?;
    read_dir(&path).ok()?;
    Some(path)
}
