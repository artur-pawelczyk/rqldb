use criterion::{criterion_group, criterion_main, Criterion, black_box};
use rqldb::{*, dsl::TupleBuilder};

enum DbType { PrimaryIndex, NoIndex }

fn create_database(t: DbType) -> Database {
    let mut db = Database::default();
    let mut def = Definition::relation("document");
    match t {
        DbType::PrimaryIndex => { def = def.indexed_attribute("id", Type::NUMBER); },
        DbType::NoIndex => { def = def.attribute("id", Type::NUMBER); },
    }

    def = def.attribute("type", Type::NUMBER)
        .attribute("title", Type::TEXT)
        .attribute("content", Type::TEXT);

    db.define(&def);

    db
}

fn query_single_number(db: &Database, query: &Query, elem: &str) -> Option<i32> {
    let result = db.execute_query(query).unwrap();
    let first = result.tuples().next().expect("Expecting one result");
    first.element(elem).and_then(|e| <i32>::try_from(e).ok())
}

fn benchmark_insert(c: &mut Criterion) {
    c.bench_function("insert", |b| b.iter_with_setup(|| {
        let db = create_database(DbType::PrimaryIndex);
        let ids: Vec<String> = (1..1000).map(|i| i.to_string()).collect();

        (db, ids)
    }, |(db, ids)| {
        for id in ids {
            let query = Query::tuple(TupleBuilder::new()
                                     .inferred("id", &id)
                                     .inferred("type", "12")
                                     .inferred("title", "example_doc")
                                     .inferred("content", "the content")
            ).insert_into("document");
            db.execute_query(&query).unwrap();
        }
    }));
}

fn benchmark_insert_without_index(c: &mut Criterion) {
    c.bench_function("insert no index", |b| b.iter_with_setup(|| {
        let db = create_database(DbType::NoIndex);
        let ids: Vec<String> = (1..1000).map(|i| i.to_string()).collect();

        (db, ids)
    }, |(db, ids)| {
        for id in ids {
            let query = Query::tuple(TupleBuilder::new()
                                     .inferred("id", &id)
                                     .inferred("type", "12")
                                     .inferred("title", "example_doc")
                                     .inferred("content", "the content")
            ).insert_into("document");
            db.execute_query(&query).unwrap();
        }
    }));
}

fn benchmark_filter(c: &mut Criterion) {
    let db = create_database(DbType::PrimaryIndex);
    for i in 1..100000 {
        let id = i.to_string();
        let query = Query::tuple(TupleBuilder::new()
                                 .inferred("id", &id)
                                 .inferred("type", "12")
                                 .inferred("title", "example_doc")
                                 .inferred("content", "the content")
        ).insert_into("document");
        db.execute_query(&query).unwrap();
    }

    let query = Query::scan("document").filter("document.id", Operator::EQ, "100");
    c.bench_function("filter", |b| b.iter(|| db.execute_query(&query).unwrap()));
}

fn benchmark_index_search(c: &mut Criterion) {
    let db = create_database(DbType::PrimaryIndex);
    for i in 1..100000 {
        let id = i.to_string();
        let query = Query::tuple(TupleBuilder::new()
                                 .inferred("id", &id)
                                 .inferred("type", "12")
                                 .inferred("title", "example_doc")
                                 .inferred("content", "the content")
        ).insert_into("document");
        db.execute_query(&query).unwrap();
    }

    let query = Query::scan_index("document.id", Operator::EQ, "100");
    c.bench_function("index search", |b| b.iter(|| db.execute_query(&query).unwrap()));
}

fn benchmark_count(c: &mut Criterion) {
    let db = create_database(DbType::PrimaryIndex);

    for i in 0..1_000_000 {
        let id = i.to_string();
        let query = Query::tuple(TupleBuilder::new()
                                 .inferred("id", &id)
                                 .inferred("type", "2")
                                 .inferred("title", "example_doc")
                                 .inferred("content", "the content")
        ).insert_into("document");
        db.execute_query(&query).unwrap();
    }

    let count_query = Query::scan("document").count();
    c.bench_function("count", |b| b.iter(|| query_single_number(&db, &count_query, "count")));
}

fn benchmark_read_results(c: &mut Criterion) {
    let db = create_database(DbType::PrimaryIndex);

    for i in 0..1_000_000 {
        let id = i.to_string();
        let query = Query::tuple(TupleBuilder::new()
                                 .inferred("id", &id)
                                 .inferred("type", "2")
                                 .inferred("title", "example_doc")
                                 .inferred("content", "the content")
        ).insert_into("document");
        db.execute_query(&query).unwrap();
    }

    c.bench_function("read_results", |b| b.iter(|| {
        let query = Query::scan("document").select_all();
        let result = db.execute_query(&query).unwrap();
        for tuple in result.tuples() {
            black_box(tuple);
        }
    }));
}

fn benchmark_parse(c: &mut Criterion) {
    let query = "scan example | join example.id other.id | filter example.value > 5 | insert_into foo";

    c.bench_function("parse query", |b| b.iter(|| parse_query(query).unwrap()));
}

fn benchmark_query_to_string(c: &mut Criterion) {
    let scan_query = Query::scan("document").join("document.type_id", "type.id").filter("document.id", Operator::EQ, "1");
    let insert_query = Query::tuple(&[("id", "1"), ("first", "foo"), ("second", "bar")]).insert_into("document");

    c.bench_function("dump scan query", |b| b.iter(|| scan_query.to_string()));
    c.bench_function("dump insert query", |b| b.iter(|| insert_query.to_string()));
}

criterion_group!(benches,
                 benchmark_insert,
                 benchmark_insert_without_index,
                 benchmark_count,
                 benchmark_read_results,
                 benchmark_parse,
                 benchmark_filter,
                 benchmark_index_search,
                 benchmark_query_to_string,
);

criterion_main!(benches);
