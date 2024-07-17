use std::{fs::{create_dir_all, DirBuilder, File}, io::{self, Cursor}, os::unix::fs::OpenOptionsExt, path::{Path, PathBuf}};

use rqldb::{schema::Schema, Database, EventSource};
use rqldb_persist::{read_schema, write_schema};

pub struct LiveStorage {
    dir: PathBuf
}

impl LiveStorage {
    fn create_db(&self) -> io::Result<Database> {
        create_dir_all(&self.dir)?;

        let mut db = self.schema_file()
            .map(|file| read_database(&file))
            .unwrap_or_else(|| Ok(Database::default()))?;

        let mut schema_file = self.dir.clone();
        schema_file.push("schema");
        db.on_define_relation(move |db, _| {
            let mut f = File::options()
                .create(true)
                .write(true)
                .open(&schema_file)
                .unwrap();

            write_schema(&mut f, db.schema()).unwrap();
        });

        Ok(db)
    }

    fn schema_file(&self) -> Option<PathBuf> {
        let mut file = self.dir.clone();
        file.push("schema");

        if file.is_file() {
            Some(file)
        } else {
            None
        }
    }
}

fn read_database(file: &Path) -> io::Result<Database> {
    let mut schema = Schema::default();
    let mut relations = read_schema(File::open(file)?).unwrap();
    relations.sort_by_key(|x| x.id);
    for rel in relations {
        rel.columns.iter().fold(schema.create_table(&rel.name), |acc, col| {
            if col.indexed {
                acc.indexed_column(&col.name, col.kind)
            } else {
                acc.column(&col.name, col.kind)
            }
        }).add();
    }

    Ok(Database::with_schema(schema))
}

#[cfg(test)]
mod tests {
    use std::error::Error;

    use rqldb::{Definition, Type};
    use tempfile::tempdir;

    use super::*;

    #[test]
    fn test_init_empty_db() -> Result<(), Box<dyn Error>> {
        let storage = LiveStorage { dir: tempdir()?.path().into() };
        let db = storage.create_db()?;
        assert!(db.schema().relations.is_empty());

        Ok(())
    }

    #[test]
    fn test_restore_db() -> Result<(), Box<dyn Error>> {
        let dir = tempdir()?;
        let storage = LiveStorage { dir: dir.path().into() };
        let mut db = storage.create_db()?;
        db.define(&Definition::relation("document").attribute("id", Type::NUMBER));

        let storage = LiveStorage { dir: dir.path().into() };
        let db = storage.create_db()?;
        assert!(db.schema().find_relation("document").is_some());

        Ok(())
    }
}
