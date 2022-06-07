use criterion::{black_box, criterion_group, criterion_main, Criterion};
use relational_nosql_lib::create::CreateRelationCommand;
use relational_nosql_lib::select::SelectQuery;
use relational_nosql_lib::schema::Type;
use relational_nosql_lib::db::Database;

fn create_database() -> Database {
    let mut db = Database::default();
    db.execute_create(&CreateRelationCommand::with_name("document")
                      .column("id", Type::NUMBER)
                      .column("type", Type::NUMBER)
                      .column("title", Type::TEXT)
                      .column("content", Type::TEXT));

    db
}

fn benchmark_insert(c: &mut Criterion) {
    let mut db = create_database();
    let query = SelectQuery::tuple(&["1", "2", "example_doc", "the_content"]).insert_into("document");
    c.bench_function("insert", |b| b.iter(|| db.execute_mut_query(&query).unwrap()));
}

criterion_group!(benches, benchmark_insert);
criterion_main!(benches);

