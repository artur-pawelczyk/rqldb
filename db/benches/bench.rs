use criterion::{criterion_group, criterion_main, Criterion};
use rqldb::*;

fn create_database<'a>() -> Database<'a> {
    let mut db = Database::default();
    db.execute_create(&Command::create_table("document")
                      .indexed_column("id", Type::NUMBER)
                      .column("type", Type::NUMBER)
                      .column("title", Type::TEXT)
                      .column("content", Type::TEXT));

    db
}

fn query_single_number(db: &Database, query: &Query) -> Option<i32> {
    let result = db.execute_query(query).unwrap();
    result.results().get(0)
        .and_then(|t| t.cell_at(0))
        .and_then(|c| c.as_number())
}

fn benchmark_insert(c: &mut Criterion) {
    c.bench_function("insert", |b| b.iter_with_setup(|| {
        let db = create_database();
        let ids: Vec<String> = (1..1000).map(|i| i.to_string()).collect();

        (db, ids)
    }, |(db, ids)| {
        for id in ids {
            db.execute_query(&Query::tuple(&[&id, "12", "example_doc", "the content"]).insert_into("document")).unwrap();
        }
    }));
}

fn benchmark_filter(c: &mut Criterion) {
    let db = create_database();
    for i in 1..100000 {
        db.execute_query(&Query::tuple(&[i.to_string().as_str(), "12", "example_doc", "the content"]).insert_into("document")).unwrap();
    }

    let query = Query::scan("document").filter("document.id", Operator::EQ, "100");
    c.bench_function("filter", |b| b.iter(|| db.execute_query(&query).unwrap()));
}

fn benchmark_index_search(c: &mut Criterion) {
    let db = create_database();
    for i in 1..100000 {
        db.execute_query(&Query::tuple(&[i.to_string().as_str(), "12", "example_doc", "the content"]).insert_into("document")).unwrap();
    }

    let query = Query::scan_index("document.id", Operator::EQ, "100");
    c.bench_function("index search", |b| b.iter(|| db.execute_query(&query).unwrap()));
}

fn benchmark_count(c: &mut Criterion) {
    let db = create_database();

    // TODO: These are duplicated tuples
    let query = Query::tuple(&["1", "2", "example_doc", "the_content"]).insert_into("document");
    for _ in 0..1_000_000 {
        db.execute_query(&query).unwrap();
    }

    let count_query = Query::scan("document").count();
    c.bench_function("count", |b| b.iter(|| query_single_number(&db, &count_query)));
}

fn benchmark_parse(c: &mut Criterion) {
    let query = "scan example | join other example.id other.id | filter example.value > 5 | insert_into foo";

    c.bench_function("parse query", |b| b.iter(|| parse_query(query).unwrap()));
}

fn benchmark_query_to_string(c: &mut Criterion) {
    let scan_query = Query::scan("document").join("type", "document.type_id", "type.id").filter("document.id", Operator::EQ, "1");
    let insert_query = Query::tuple(&["1", "foo", "bar"]).insert_into("document");

    c.bench_function("dump scan query", |b| b.iter(|| scan_query.to_string()));
    c.bench_function("dump insert query", |b| b.iter(|| insert_query.to_string()));

}

criterion_group!(benches, benchmark_insert, benchmark_count, benchmark_parse, benchmark_filter, benchmark_index_search, benchmark_query_to_string);
criterion_main!(benches);
