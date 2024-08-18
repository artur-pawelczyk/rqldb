mod import;

use core::fmt;
use std::{error::Error, fs::File, io::Read};

use rqldb::{Database, Definition, Type};

use crate::import::Import;

fn main() -> Result<(), Box<dyn Error>> {
    let mut db = Database::default();
    db.define(&Definition::relation("state")
                      .indexed_attribute("id", Type::TEXT)
                      .attribute("name", Type::TEXT));

    db.define(&Definition::relation("county")
                      .indexed_attribute("id", Type::NUMBER)
                      .attribute("name", Type::TEXT)
                      .attribute("state_id", Type::TEXT));

    db.define(&Definition::relation("city")
                      .indexed_attribute("id", Type::NUMBER)
                      .attribute("name", Type::TEXT)
                      .attribute("state_id", Type::TEXT)
                      .attribute("county_id", Type::NUMBER)
                      .attribute("population", Type::NUMBER));

    let mut csv = String::new();
    if let Some(file) = std::env::args().nth(1) {
        File::options().read(true).open(&file)?.read_to_string(&mut csv)?;
    } else {
        std::io::stdin().read_to_string(&mut csv)?;
    }
    
    Import::into_table("state")
        .column("state_id", "id")
        .column("state_name", "name")
        .import_string(&csv, |query| {
            db.insert(&query).unwrap();
        })?;

    Import::into_table("county")
        .column("county_fips", "id")
        .column("county_name", "name")
        .column("state_id", "state_id")
        .import_string(&csv, |query| {
            db.insert(&query).unwrap();
        })?;

    Import::into_table("city")
        .column("id", "id")
        .column("city_ascii", "name")
        .column("state_id", "state_id")
        .column("county_fips", "county_id")
        .column("population", "population")
        .import_string(&csv, |query| {
            db.insert(&query).unwrap();
        })?;

    db.dump_all(&mut StdoutWrite)?;

    Ok(())
}

struct StdoutWrite;
impl fmt::Write for StdoutWrite {
    fn write_str(&mut self, s: &str) -> fmt::Result {
        print!("{s}");
        Ok(())
    }
}
