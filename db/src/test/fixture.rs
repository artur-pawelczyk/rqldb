use crate::{dsl::Insert, Database, Definition, Type};

pub struct Document(pub Flavor);
impl GenerateData for Document {
    fn generate_schema(&self, db: &mut Database) {
        db.define(&Definition::relation("document")
                  .attribute("id", Type::NUMBER)
                  .attribute("content", Type::TEXT));
    }

    fn generate_data(&self, db: &mut Database) {
        if let Flavor::Size(n) = self.0 {
            for i in 1..=n {
                db.insert(&Insert::insert_into("document")
                          .element("id", i)
                          .element("content", format!("example {i}"))).unwrap();
            }
        }
    }
}

pub struct DocumentWithType;
impl GenerateData for DocumentWithType {
    fn generate_schema(&self, db: &mut Database) {
        db.define(&Definition::relation("document")
                  .attribute("id", Type::NUMBER)
                  .attribute("content", Type::TEXT)
                  .attribute("type_id", Type::NUMBER));

        db.define(&Definition::relation("type")
                  .attribute("id", Type::NUMBER)
                  .attribute("name", Type::TEXT));
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

pub trait Fixture {
    fn generate_dataset(self, d: impl GenerateData) -> Self;
}

impl Fixture for Database {
    fn generate_dataset(mut self, d: impl GenerateData) -> Self {
        d.generate_schema(&mut self);
        d.generate_data(&mut self);
        self
    }
}

pub trait GenerateData {
    fn generate_schema(&self, db: &mut Database);
    fn generate_data(&self, _: &mut Database) {
    }
}
