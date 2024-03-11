use std::path::Path;

use criterion::{criterion_group, criterion_main, Criterion};
use rqldb::*;
use rqldb_persist::*;

fn create_database() -> Database {
    let mut db = Database::default();
    db.execute_create(&Command::create_table("document")
                      .indexed_column("id", Type::NUMBER)
                      .column("type", Type::NUMBER)
                      .column("title", Type::TEXT)
                      .column("content", Type::TEXT));

    for i in 1..10_000 {
        let id = i.to_string();
        let title = "title".to_string() + &id;
        let content = "content".to_string() + &id;
        db.execute_query(&Query::tuple(&[&id, "1", &title, &content]).insert_into("document")).unwrap();
    }

    db
}

fn benchmark_write_object(c: &mut Criterion) {
    let db = create_database();
    let mut persist = FilePersist::new(Path::new("/dev/null"));

    c.bench_function("write object", |b| b.iter(|| {
        persist.write(&db).unwrap();
    }));
}

criterion_group!(benches, benchmark_write_object);
criterion_main!(benches);
