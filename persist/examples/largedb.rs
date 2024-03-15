use std::time::SystemTime;

use rqldb::{Database, Command, Type, Query, dsl::TupleBuilder};
use rqldb_persist::{Persist, TempFilePersist};

fn main() {
    let mut db = Database::default();
    db.execute_create(&Command::create_table("document")
                      .indexed_column("id", Type::NUMBER)
                      .column("title", Type::TEXT)
                      .column("content", Type::TEXT));


    let mut last_time = SystemTime::now();
    for i in 0..50_000_000 {
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
            println!("Inserted {} tuples; took {:?}", i, SystemTime::now().duration_since(last_time));
            last_time = SystemTime::now();
        }
    }

    println!("Saving...");
    let mut file = TempFilePersist::new();
    file.write(&db).unwrap();
}
