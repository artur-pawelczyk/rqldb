use rqldb::{parse_query, parse_command};
use rqldb::db::Database;

fn prepare_db() -> Database {
    let mut db = Database::default();

    db.execute_create(&parse_command("create_table document id::NUMBER::KEY title::TEXT content::TEXT type::NUMBER").unwrap());
    db.execute_create(&parse_command("create_table type id::NUMBER name::TEXT").unwrap());

    let insert = parse_query("tuple 1 artictle | insert_into type").unwrap();
    db.execute_query(&insert).unwrap();
    let insert = parse_query("tuple 2 blog | insert_into type").unwrap();
    db.execute_query(&insert).unwrap();
    let insert = parse_query("tuple 3 book | insert_into type").unwrap();
    db.execute_query(&insert).unwrap();
    let insert = parse_query("tuple 1 title content 2 | insert_into document").unwrap();
    db.execute_query(&insert).unwrap();
    let insert = parse_query("tuple 2 title2 content2 3 | insert_into document").unwrap();
    db.execute_query(&insert).unwrap();

    db
}

#[test]
fn test_basic_queries() {
    let db = prepare_db();

    let query = parse_query("scan document").unwrap();
    let result = db.execute_query(&query).unwrap();
    assert_eq!(result.results().len(), 2);

    let query = parse_query("scan document | filter document.id = 2").unwrap();
    let result = db.execute_query(&query).unwrap();
    assert_eq!(result.results().len(), 1);
    assert_eq!(result.results().get(0).unwrap().cell_by_name("document.title").unwrap().as_string(), "title2");

    let query = parse_query("scan document | filter document.id > 0 | filter document.id < 10").unwrap();
    let result = db.execute_query(&query).unwrap();
    assert_eq!(result.results().len(), 2);
}

#[test]
fn test_tuple_queries() {
    let db = prepare_db();

    let query = parse_query("tuple 1 \"some text\" | filter 0 = 1").unwrap();
    let result = db.execute_query(&query).unwrap();
    assert_eq!(result.results().len(), 1);

    let query = parse_query("tuple 1 \"some text\" | filter 1 = \"some text\"").unwrap();
    let result = db.execute_query(&query).unwrap();
    assert_eq!(result.results().len(), 1);


    let query = parse_query("tuple 1 \"some text\" | filter 0 = 2").unwrap();
    let result = db.execute_query(&query).unwrap();
    assert!(result.results().is_empty());
}

#[test]
fn test_single_join() {
    let db = prepare_db();

    let query = parse_query("scan document | join type document.type type.id").unwrap();
    let result = db.execute_query(&query).unwrap();
    assert_eq!(result.results().len(), 2);
}

#[test]
fn test_update() {
    let db = prepare_db();

    let insert = parse_query("tuple 1 updated_title content 2 | insert_into document").unwrap();
    db.execute_query(&insert).unwrap();

    let query = parse_query("scan document | filter document.id = 1").unwrap();
    let result = db.execute_query(&query).unwrap();
    assert_eq!(result.results().get(0).unwrap().cell_by_name("document.title").unwrap().as_string(), "updated_title");
}
