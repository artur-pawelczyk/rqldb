use criterion::{criterion_group, criterion_main, Criterion};
use rqldb::*;

fn create_database() -> Database {
    let mut db = Database::default();
    db.execute_create(&Command::create_table("document")
                      .column("id", Type::NUMBER)
                      .column("type", Type::NUMBER)
                      .column("title", Type::TEXT)
                      .column("content", Type::TEXT));

    db
}

fn query_single_number(db: &Database, query: &Query) -> Option<i32> { let result =
    db.execute_query(query).unwrap();
    result.results().get(0).map(|t| t.cell_at(0)).flatten().map(|c|
    c.as_number()).flatten() }

fn benchmark_insert(c: &mut Criterion) {
    let mut db = create_database();
    let query = Query::tuple(&["1", "2", "example_doc", "the_content"]).insert_into("document");
    c.bench_function("insert", |b| b.iter(|| db.execute_mut_query(&query).unwrap()));

    let count_query = Query::scan("document").count();
    println!("count: {}", query_single_number(&db, &count_query).unwrap());
}

fn benchmark_filter(c: &mut Criterion) {
    let mut db = create_database();
    for i in 1..1000000 {
        db.execute_mut_query(&Query::tuple(&[i.to_string().as_str(), "12", "example_doc", "the content"]).insert_into("document")).unwrap();
    }

    let query = Query::scan("document").filter("document.id", Operator::EQ, "100");
    c.bench_function("filter", |b| b.iter(|| db.execute_query(&query).unwrap()));
}

fn benchmark_count(c: &mut Criterion) {
    let mut db = create_database();

    let query = Query::tuple(&["1", "2", "example_doc", "the_content"]).insert_into("document");
    for _ in 0..1000_000 {
        db.execute_mut_query(&query).unwrap();
    }

    let count_query = Query::scan("document").count();
    c.bench_function("count", |b| b.iter(|| query_single_number(&db, &count_query)));
}

fn benchmark_parse(c: &mut Criterion) {
    let query = "scan example | join other example.id other.id | filter example.value > 5 | insert_into foo";

    c.bench_function("parse query", |b| b.iter(|| parse_query(query)));
}

criterion_group!(benches, benchmark_insert, benchmark_count, benchmark_parse, benchmark_filter);
criterion_main!(benches);
