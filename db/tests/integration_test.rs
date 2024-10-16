use rqldb::{parse_definition, parse_delete, parse_insert, parse_query};
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
    let db = prepare_db();

    let query = parse_query("scan document").unwrap();
    let result = db.execute_query(&query).unwrap();
    assert_eq!(result.tuples().count(), 2);

    let query = parse_query("scan document | filter document.id = 2").unwrap();
    let result = db.execute_query(&query).unwrap();
    let mut tuples = result.tuples();
    assert_eq!(tuples.next().unwrap().element("document.title").unwrap().to_string(), "title2");
    assert!(tuples.next().is_none());

    let query = parse_query("scan document | filter document.id > 0 | filter document.id < 10").unwrap();
    let result = db.execute_query(&query).unwrap();
    assert_eq!(result.tuples().count(), 2);

    let query = parse_query("scan document | filter document.published = true").unwrap();
    let result = db.execute_query(&query).unwrap();
    assert_eq!(result.tuples().count(), 1);
}

#[test]
fn test_tuple_queries() {
    let db = prepare_db();

    let query = parse_query("tuple 0::NUMBER = 1 1::TEXT = something | filter 0 = 1").unwrap();
    let result = db.execute_query(&query).unwrap();
    assert_eq!(result.tuples().count(), 1);

    let query = parse_query("tuple 0::NUMBER = 1 1::TEXT = something | filter 1 = something").unwrap();
    let result = db.execute_query(&query).unwrap();
    assert_eq!(result.tuples().count(), 1);

    let query = parse_query("tuple 0::NUMBER = 1 1::TEXT = something | filter 0 = 2").unwrap();
    let result = db.execute_query(&query).unwrap();
    assert!(result.tuples().next().is_none());
}

#[test]
fn test_single_join() {
    let db = prepare_db();

    let query = parse_query("scan document | join document.type type.id").unwrap();
    let result = db.execute_query(&query).unwrap();
    assert_eq!(result.tuples().count(), 2);
}

#[test]
fn test_update() {
    let db = prepare_db();

    let insert = parse_insert("document id = 1 title = updated_title content = content type = 2 published = true").unwrap();
    db.insert(&insert).unwrap();

    let query = parse_query("scan document | filter document.id = 1").unwrap();
    let result = db.execute_query(&query).unwrap();
    assert_eq!(result.tuples().next().unwrap().element("document.title").unwrap().to_string(), "updated_title");
}

#[test]
fn test_delete() {
    let db = prepare_db();

    let delete = parse_delete("scan document | filter document.id = 1").unwrap();
    db.delete(&delete).unwrap();

    let after_delete = db.execute_query(&parse_query("scan document").unwrap()).unwrap();
    assert_eq!(after_delete.tuples().count(), 1);
}

#[test]
fn test_data_types() -> Result<(), Box<dyn std::error::Error>> {
    let db = prepare_db();

    let results = db.execute_query(&parse_query("scan document | filter document.id = 1")?)?;

    let tuple = results.tuples().next().unwrap();
    let elem = tuple.element("document.published").unwrap();
    assert_eq!(elem.try_into(), Ok(true));

    let elem = tuple.element("document.published").unwrap();
    assert_eq!(format!("{elem}"), "true");

    Ok(())
}
