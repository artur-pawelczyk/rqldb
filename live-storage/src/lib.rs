mod heap;
mod schema;

use std::{fs::{create_dir_all, File}, io, path::{Path, PathBuf}};

use heap::Heap;
use rqldb::{object::ObjectId, schema::Schema, Database, EventSource};
use schema::{read_schema, write_schema};

pub struct LiveStorage {
    dir: PathBuf,
}

impl LiveStorage {
    #[cfg(test)]
    fn temporary() -> io::Result<Self> {
        Ok(LiveStorage {
            dir: tempfile::tempdir()?.path().into(),
        })
    }

    pub fn new(s: &str) -> Self {
        Self { dir: PathBuf::from(s).canonicalize().unwrap() }
    }

    pub fn create_db(&self) -> io::Result<Database> {
        create_dir_all(&self.dir)?;

        let mut db = self.schema_file()
            .map(|file| read_database(&file))
            .unwrap_or_else(|| Ok(Database::default()))?;

        for rel in db.schema().relations.clone() {
            if let Some(file_path) = self.object_file(rel.id) {
                let file = File::options().read(true).open(&file_path)?;
                db.read_object(rel.id, file).unwrap();
            }
        }

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

        let dir = self.dir.clone();
        db.on_page_modified(move |obj_id, block, bytes| {
            let mut path = dir.clone();
            path.push(&obj_id.to_string());
            let mut heap = Heap::open(&path)?;
            heap.write_page(block, bytes)?;
            Ok(())
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

    fn object_file(&self, id: ObjectId) -> Option<PathBuf> {
        let mut file = self.dir.clone();
        file.push(id.to_string());
        if file.is_file() {
            Some(file)
        } else {
            None
        }
    }

    #[cfg(test)]
    fn reopen(self) -> Self {
        Self { dir: self.dir }
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

    use rqldb::{dsl::TupleBuilder, Definition, Query, Type};
    use super::*;

    #[test]
    fn test_init_empty_db() -> Result<(), Box<dyn Error>> {
        let storage = LiveStorage::temporary()?;
        let db = storage.create_db()?;
        assert!(db.schema().relations.is_empty());

        Ok(())
    }

    #[test]
    fn test_restore_schema() -> Result<(), Box<dyn Error>> {
        let storage = LiveStorage::temporary()?;
        let mut db = storage.create_db()?;
        db.define(&Definition::relation("document").attribute("id", Type::NUMBER))?;

        let storage = storage.reopen();
        let db = storage.create_db()?;
        assert!(db.schema().find_relation("document").is_some());

        Ok(())
    }

    #[test]
    fn test_restore_db()  -> Result<(), Box<dyn Error>> {
        let storage = LiveStorage::temporary()?;
        let mut db = storage.create_db()?;
        db.define(&Definition::relation("document")
                  .indexed_attribute("id", Type::NUMBER)
                  .attribute("name", Type::TEXT))?;
        db.insert(&Query::tuple(TupleBuilder::new()
                                       .inferred("id", "1")
                                       .inferred("name", "example one"))
                  .insert_into("document"))?;

        let storage = storage.reopen();
        let db = storage.create_db()?;

        let result = db.execute_query(&Query::scan("document"))?;
        let first = result.tuples().next().unwrap();
        assert_eq!(first.element("document.name").unwrap().to_string(), "example one");

        Ok(())
    }
}
