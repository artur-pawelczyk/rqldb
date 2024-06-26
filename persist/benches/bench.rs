use std::path::Path;

use criterion::{criterion_group, criterion_main, Criterion};
use rqldb::{*, dsl::TupleBuilder};
use rqldb_persist::*;

fn create_database() -> Database {
    let mut db = Database::default();
    db.define(&Definition::relation("document")
                      .indexed_attribute("id", Type::NUMBER)
                      .attribute("type", Type::NUMBER)
                      .attribute("title", Type::TEXT)
                      .attribute("content", Type::TEXT));

    for i in 1..10_000 {
        let id = i.to_string();
        let title = "title".to_string() + &id;
        let content = "content".to_string() + &id;
        let query = Query::tuple(TupleBuilder::new()
                                 .inferred("id", &id)
                                 .inferred("type", "2")
                                 .inferred("title", &title)
                                 .inferred("content", &content)
        ).insert_into("document");
        db.execute_query(&query).unwrap();
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
