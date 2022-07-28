use rqldb::{Database, Command, Type, Query};
use rqldb_persist::{Persist, TempFilePersist};

fn main() {
    let mut db = Database::default();
    db.execute_create(&Command::create_table("document")
                      .indexed_column("id", Type::NUMBER)
                      .column("title", Type::TEXT)
                      .column("content", Type::TEXT));


    for i in 0..1_000_000 {
        let title = format!("title {i}");
        let content = format!("content {i}");
        db.execute_query(&Query::tuple(&[i.to_string(), title, content]).insert_into("document")).unwrap();
    }

    let mut file = TempFilePersist::new();
    file.write(&db).unwrap();
}
