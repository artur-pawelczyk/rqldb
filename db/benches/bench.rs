use std::iter;

use criterion::{criterion_group, criterion_main, Criterion, black_box};
use rqldb::{*, dsl::TupleBuilder};

enum DbType { PrimaryIndex, NoIndex }

fn create_database(t: DbType) -> Database {
    Database::default()
        .add_document_relation(t)
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

fn benchmark_delete(c: &mut Criterion) {
    fn prepare_db() -> Database {
        let db = create_database(DbType::PrimaryIndex);
        for i in 1..100_000 {
            db.execute_query(&Query::tuple(TupleBuilder::new()
                                           .inferred("id", &i.to_string())
                                           .inferred("type", "12")
                                           .inferred("title", "example_doc")
                                           .inferred("content", "the content")
            ).insert_into("document")).unwrap();
        }

        db
    }

    c.bench_function("delete", |b| {
        b.iter_batched(|| prepare_db(), |db| {
            db.execute_query(&Query::scan("document").delete()).unwrap();
        }, criterion::BatchSize::PerIteration);
    });
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

fn benchmark_join(c: &mut Criterion) {
    let db = Database::default()
        .add_document_relation(DbType::PrimaryIndex)
        .add_type_relation();

    let types = 1..=10;
    for id in types.clone() {
        db.execute_query(&Query::build_tuple()
                         .inferred("id", &id.to_string())
                         .inferred("name", "something")
                         .build().insert_into("type")).unwrap();
    }

    for (doc_id, type_id) in iter::zip(0..1_000_000, types.cycle()) {
        db.execute_query(&Query::build_tuple()
                         .inferred("id", &doc_id.to_string())
                         .inferred("title", "something")
                         .inferred("type", &type_id.to_string())
                         .inferred("content", "the content")
                         .build().insert_into("document")).unwrap();
    }

    let query = Query::scan("document").join("document.type", "type.id");
    c.bench_function("join", |b| b.iter(|| db.execute_query(&query).unwrap()));
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
                 benchmark_delete,
                 benchmark_insert_without_index,
                 benchmark_count,
                 benchmark_join,
                 benchmark_read_results,
                 benchmark_parse,
                 benchmark_filter,
                 benchmark_index_search,
                 benchmark_query_to_string,
);

criterion_main!(benches);

trait DatabaseFixture {
    fn add_document_relation(self, t: DbType) -> Self;
    fn add_type_relation(self) -> Self;
}

impl DatabaseFixture for Database {
    fn add_document_relation(mut self, t: DbType) -> Self {
        let mut def = Definition::relation("document");
        match t {
            DbType::PrimaryIndex => { def = def.indexed_attribute("id", Type::NUMBER); },
            DbType::NoIndex => { def = def.attribute("id", Type::NUMBER); },
        }

        def = def.attribute("type", Type::NUMBER)
            .attribute("title", Type::TEXT)
            .attribute("content", Type::TEXT);

        self.define(&def);

        self
    }

    fn add_type_relation(mut self) -> Self {
        self.define(&Definition::relation("type")
                    .attribute("id", Type::NUMBER)
                    .attribute("name", Type::TEXT));
        self
    }
}
