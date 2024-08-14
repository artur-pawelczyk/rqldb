use std::error::Error;

use rqldb::{parse, Database};

const DEF: &'static str = ".define";
const INSERT: &'static str = ".insert";

#[test]
fn test_example_code() -> Result<(), Box<dyn Error>> {
    let mut db = Database::default();

    let file = std::fs::read("../examples/example.query")?;
    for line in String::from_utf8(file).unwrap().lines().filter(|s| !s.is_empty()).map(str::trim) {
        if line.starts_with(DEF) {
            let command = parse::parse_definition(&line[DEF.len()..])?;
            db.define(&command);
        } else if line.starts_with(INSERT) {
            let command = parse::parse_insert(&line[INSERT.len()..])?;
            db.insert(&command)?;
        } else {
            let query = parse::parse_query(line)?;
            db.execute_query(&query)?;
        }
    }

    Ok(())
}
