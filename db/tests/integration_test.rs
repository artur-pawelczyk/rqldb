use rqldb::interpret::Interpreter;
use rqldb::{parse_definition, parse_insert};
use rqldb::Database;

fn prepare_db() -> Database {
    let mut db = Database::default();

    db.define(&parse_definition("relation document
id::NUMBER::KEY
title::TEXT
content::TEXT
type::NUMBER
published::BOOLEAN").unwrap()).unwrap();
    db.define(&parse_definition("relation type id::NUMBER name::TEXT").unwrap()).unwrap();

    let insert = parse_insert("type id = 1 name = artictle").unwrap();
    db.insert(&insert).unwrap();
    let insert = parse_insert("type id = 2 name = blog").unwrap();
    db.insert(&insert).unwrap();
    let insert = parse_insert("type id = 3 name = book | insert_into type").unwrap();
    db.insert(&insert).unwrap();
    let insert = parse_insert("document id = 1 title = title1 content = content1 type = 2 published = true").unwrap();
    db.insert(&insert).unwrap();
    let insert = parse_insert("document id = 2 title = title2 content = content2 type = 2 published = false").unwrap();
    db.insert(&insert).unwrap();

    db
}

#[test]
fn test_basic_queries() {
    let db = Interpreter::with_database(prepare_db());

    let query = "scan document";
    let result = db.run_query(query).unwrap();
    assert_eq!(result.tuples().count(), 2);

    let query = "scan document | filter document.id = 2";
    let result = db.run_query(query).unwrap();
    let mut tuples = result.tuples();
    assert_eq!(tuples.next().unwrap().element("document.title").unwrap().to_string(), "title2");
    assert!(tuples.next().is_none());

    let query = "scan document | filter document.id > 0 | filter document.id < 10";
    let result = db.run_query(query).unwrap();
    assert_eq!(result.tuples().count(), 2);

    let query = "scan document | filter document.published = true";
    let result = db.run_query(query).unwrap();
    assert_eq!(result.tuples().count(), 1);
}

#[test]
fn test_tuple_queries() {
    let db = Interpreter::with_database(prepare_db());

    let result = db.run_query("tuple 0::NUMBER = 1 1::TEXT = something | filter 0 = 1").unwrap();
    assert_eq!(result.tuples().count(), 1);

    let result = db.run_query("tuple 0::NUMBER = 1 1::TEXT = something | filter 1 = something").unwrap();
    assert_eq!(result.tuples().count(), 1);

    let result = db.run_query("tuple 0::NUMBER = 1 1::TEXT = something | filter 0 = 2").unwrap();
    assert!(result.tuples().next().is_none());
}

#[test]
fn test_single_join() {
    let db = Interpreter::with_database(prepare_db());

    let result = db.run_query("scan document | join document.type type.id").unwrap();
    assert_eq!(result.tuples().count(), 2);
}

#[test]
fn test_update() {
    let db = Interpreter::with_database(prepare_db());

    let _ = db.run_query(".insert document id = 1 title = updated_title content = content type = 2 published = true");

    let result = db.run_query("scan document | filter document.id = 1").unwrap();
    assert_eq!(result.tuples().next().unwrap().element("document.title").unwrap().to_string(), "updated_title");
}

#[test]
fn test_delete() {
    let db = Interpreter::with_database(prepare_db());

    let _ = db.run_query(".delete scan document | filter document.id = 1");

    let after_delete = db.run_query("scan document").unwrap();
    assert_eq!(after_delete.tuples().count(), 1);
}

#[test]
fn test_data_types() -> Result<(), Box<dyn std::error::Error>> {
    let db = Interpreter::with_database(prepare_db());

    let results = db.run_query("scan document | filter document.id = 1")?;

    let tuple = results.tuples().next().unwrap();
    let elem = tuple.element("document.published").unwrap();
    assert_eq!(elem.try_into(), Ok(true));

    let elem = tuple.element("document.published").unwrap();
    assert_eq!(format!("{elem}"), "true");

    Ok(())
}
