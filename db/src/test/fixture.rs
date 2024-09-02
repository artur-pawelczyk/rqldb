use crate::{dsl::Insert, Database, Definition, Type};

pub struct Document(pub Flavor);
impl Fixture for Document {
    fn generate_schema(&self, db: &mut Database) {
        db.define(&Definition::relation("document")
                  .attribute("id", Type::NUMBER)
                  .attribute("content", Type::TEXT)
        ).unwrap();
    }

    fn generate_data(&self, db: &mut Database) {
        if let Flavor::Size(n) = self.0 {
            for i in 1..=n {
                db.insert(&Insert::insert_into("document")
                          .element("id", i)
                          .element("content", format!("example {i}"))
                ).unwrap();
            }
        }
    }
}

pub struct DocumentWithSize(pub Flavor);
impl Fixture for DocumentWithSize {
    fn generate_schema(&self, db: &mut Database) {
        db.define(&Definition::relation("document")
                  .attribute("id", Type::NUMBER)
                  .attribute("content", Type::TEXT)
                  .attribute("size", Type::NUMBER)
        ).unwrap();
    }

    fn generate_data(&self, db: &mut Database) {
        if let Flavor::Size(n) = self.0 {
            for i in 1..=n {
                db.insert(&Insert::insert_into("document")
                          .element("id", i)
                          .element("content", format!("example {i}"))
                          .element("size", i)
                ).unwrap();
            }
        }
    }
}

pub struct DocumentWithType;
impl Fixture for DocumentWithType {
    fn generate_schema(&self, db: &mut Database) {
        db.define(&Definition::relation("document")
                  .attribute("id", Type::NUMBER)
                  .attribute("content", Type::TEXT)
                  .attribute("type_id", Type::NUMBER)
        ).unwrap();

        db.define(&Definition::relation("type")
                  .attribute("id", Type::NUMBER)
                  .attribute("name", Type::TEXT)
        ).unwrap();
    }

    fn generate_data(&self, db: &mut Database) {
        db.insert(&Insert::insert_into("document").element("id", 1).element("content", "example").element("type_id", 2)).unwrap();
        db.insert(&Insert::insert_into("document").element("id", 2).element("content", "no type").element("type_id", 3)).unwrap();
        db.insert(&Insert::insert_into("type").element("id", 1).element("name", "type_a")).unwrap();
        db.insert(&Insert::insert_into("type").element("id", 2).element("name", "type_b")).unwrap();
    }
}

pub enum Flavor {
    SchemaOnly,
    Size(u32),
}

pub trait GenerateDataset {
    fn generate_dataset(self, d: impl Fixture) -> Self;
}

impl GenerateDataset for Database {
    fn generate_dataset(mut self, d: impl Fixture) -> Self {
        d.generate_schema(&mut self);
        d.generate_data(&mut self);
        self
    }
}

pub trait Fixture {
    fn generate_schema(&self, db: &mut Database);
    fn generate_data(&self, _: &mut Database) {
    }
}
