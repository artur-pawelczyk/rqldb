use std::{error::Error, fs::{read_dir, File}, io::Read as _, path::Path};

use rqldb::{parse, Database};

const DEF: &'static str = ".define";
const INSERT: &'static str = ".insert";

#[test]
fn test_example_code() -> Result<(), Box<dyn Error>> {
    for entry in read_dir("../examples/")? {
        let file_path = entry?.path();
        if file_path.extension().map(|s| s == "query").unwrap_or(false) {
            validate_file(&file_path)?;
        }
    }

    Ok(())
}

fn validate_file(path: &Path) -> Result<(), Box<dyn Error>> {
    let mut db = Database::default();
    let mut contents = String::new();
    File::open(path)?.read_to_string(&mut contents)?;
    for line in contents.lines().filter(|s| !s.is_empty()).map(str::trim) {
        if line.starts_with(DEF) {
            let command = parse::parse_definition(&line[DEF.len()..])?;
            db.define(&command)?;
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
