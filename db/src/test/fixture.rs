use crate::{dsl::Insert, Database, Definition, Type};

#[derive(Default)]
pub struct Dataset(Vec<Box<dyn Fixture>>);
impl Dataset {
    pub fn add(mut self, f: impl Fixture + 'static) -> Self {
        self.0.push(Box::from(f));
        self
    }

    pub fn generate(self, mut db: Database) -> Database {
        let f = &self.0[0];
        f.generate_schema(&mut db);
        f.generate_data(&mut db);
        db
    }

    pub fn db(self) -> Database {
        self.generate(Default::default())
    }
}

pub struct Document(usize);

impl Document {
    pub fn empty() -> Self {
        Self(0)
    }

    pub fn size(n: usize) -> Self {
        Self(n)
    }
 }

impl Fixture for Document {
    fn generate_schema(&self, db: &mut Database) {
        db.define(&Definition::relation("document")
                  .attribute("id", Type::NUMBER)
                  .attribute("content", Type::TEXT)
        ).unwrap();
    }

    fn generate_data(&self, db: &mut Database) {
        for i in 1..=self.0 {
            db.insert(&Insert::insert_into("document")
                      .element("id", i)
                      .element("content", format!("example {i}"))
            ).unwrap();
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

pub struct DocType(usize);
impl Fixture for DocType {
    fn generate_schema(&self, db: &mut Database) {
        db.define(&Definition::relation("type")
                  .attribute("id", Type::NUMBER)
                  .attribute("name", Type::TEXT)
        ).unwrap();
    }

    fn generate_data(&self, db: &mut Database) {
        let chars = "abcdefgh";
        for (id, ch) in chars.char_indices().cycle().take(self.0) {
            db.insert(&Insert::insert_into("type")
                      .element("id", id)
                      .element("name", format!("type_{ch}"))).unwrap();
        }
    }

    fn before_define<'a>(&self, mut def: Definition) -> Definition {
        if def.name == "document" {
            def = def.attribute("type_id", Type::NUMBER);
        }

        def
    }
}

pub enum Flavor {
    SchemaOnly,
    Size(u32),
}

pub trait Fixture {
    fn generate_schema(&self, db: &mut Database);
    fn generate_data(&self, _: &mut Database) {
    }

    fn before_define<'a>(&self, def: Definition) -> Definition {
        def
    }
}
