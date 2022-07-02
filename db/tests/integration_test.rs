use rqldb::{parse_query, parse_command, Database};

fn prepare_db() -> Database {
    let mut db = Database::default();

    let command = parse_command("create_table document id::NUMBER title::TEXT content::TEXT").unwrap();
    db.execute_create(&command);

    let insert = parse_query("tuple 1 title content | insert_into document").unwrap();
    db.execute_query(&insert);

    db
}

#[test]
fn test_basic_queries() {
    let db = prepare_db();

    let query = parse_query("scan document").unwrap();
    let result = db.execute_query(&query).unwrap();
    assert_eq!(result.results().len(), 1);
}
