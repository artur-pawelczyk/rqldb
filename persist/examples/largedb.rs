use std::time::SystemTime;

use rqldb::{Database, Command, Type, Query};
use rqldb_persist::{Persist, TempFilePersist};

fn main() {
    let mut db = Database::default();
    db.execute_create(&Command::create_table("document")
                      .indexed_column("id", Type::NUMBER)
                      .column("title", Type::TEXT)
                      .column("content", Type::TEXT));


    let mut last_time = SystemTime::now();
    for i in 0..50_000_000 {
        let title = format!("title {i}");
        let content = format!("content {i}");
        db.execute_query(&Query::tuple_untyped(&[&i.to_string(), &title, &content]).insert_into("document")).unwrap();

        if i % 100_000 == 0 {
            println!("Inserted {} tuples; took {:?}", i, SystemTime::now().duration_since(last_time));
            last_time = SystemTime::now();
        }
    }

    println!("Saving...");
    let mut file = TempFilePersist::new();
    file.write(&db).unwrap();
}
