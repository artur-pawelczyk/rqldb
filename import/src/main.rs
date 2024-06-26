mod import;

use core::fmt;
use std::{error::Error, io::Read};

use rqldb::{Database, Command, Type};

use crate::import::Import;

fn main() -> Result<(), Box<dyn Error>> {
    let mut db = Database::default();
    db.execute_create(&Command::create_table("state")
                      .indexed_column("id", Type::TEXT)
                      .column("name", Type::TEXT));

    db.execute_create(&Command::create_table("county")
                      .indexed_column("id", Type::NUMBER)
                      .column("name", Type::TEXT)
                      .column("state_id", Type::TEXT));

    db.execute_create(&Command::create_table("city")
                      .indexed_column("id", Type::NUMBER)
                      .column("name", Type::TEXT)
                      .column("state_id", Type::TEXT)
                      .column("county_id", Type::NUMBER));

    let mut csv = String::new();
    std::io::stdin().read_to_string(&mut csv).unwrap();
    
    Import::into_table("state")
        .column("state_id", "id")
        .column("state_name", "name")
        .import_string(&csv, |query| {
            db.execute_query(&query).unwrap();
        })?;

    Import::into_table("county")
        .column("county_fips", "id")
        .column("county_name", "name")
        .column("state_id", "state_id")
        .import_string(&csv, |query| {
            db.execute_query(&query).unwrap();
        })?;

    Import::into_table("city")
        .column("id", "id")
        .column("city_ascii", "name")
        .column("state_id", "state_id")
        .column("county_fips", "county_id")
        .import_string(&csv, |query| {
            db.execute_query(&query).unwrap();
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
