use std::error::Error;

use rqldb::{parse, Database};

#[test]
fn test_example_code() -> Result<(), Box<dyn Error>> {
    let mut db = Database::default();

    let file = std::fs::read("../examples/example.query")?;
    for line in String::from_utf8(file).unwrap().lines().filter(|s| !s.is_empty()).map(str::trim) {
        if line.starts_with("create") {
            let command = parse::parse_definition(line)?;
            db.execute_create(&command);
        } else {
            let query = parse::parse_query(line)?;
            db.execute_query(&query)?;
        }
    }

    Ok(())
}
