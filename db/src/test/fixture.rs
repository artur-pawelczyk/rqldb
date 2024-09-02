use std::{collections::BTreeMap, iter};

use crate::{dsl::Insert, Database, Definition};

use super::gen::Generator;

#[derive(Default)]
pub struct Dataset(Vec<Box<dyn Fixture>>);
impl Dataset {
    pub fn add(mut self, f: impl Fixture + 'static) -> Self {
        self.0.push(Box::from(f));
        self
    }

    pub fn generate(self, mut db: Database) -> Database {
        let relations = self.0.iter().flat_map(|f| f.generate_schema()).collect::<Vec<_>>();
        let relations = relations.into_iter().map(|mut def| {
            for f in &self.0 {
                def = f.before_define(def);
            }
            def
        }).collect::<Vec<_>>();

        for rel in &relations {
            db.define(&rel.into()).unwrap();
        }

        for f in self.0 {
            f.generate_data(&mut db, &relations);
        }

        db
    }

    pub fn db(self) -> Database {
        self.generate(Default::default())
    }
}

pub struct Relation(&'static str, BTreeMap<&'static str, Generator>);

impl Relation {
    fn name(name: &'static str) -> Self {
        Self(name, BTreeMap::new())
    }

    fn attribute(mut self, name: &'static str, gen: Generator) -> Self {
        self.1.insert(name, gen);
        self
    }
}

impl From<&Relation> for Definition {
    fn from(rel: &Relation) -> Self {
        let mut def = Definition::relation(rel.0);
        for (name, gen) in &rel.1 {
            def = def.attribute(name, gen.kind());
        }

        def
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
    fn generate_schema(&self) -> Box<dyn Iterator<Item = Relation>> {
        Box::new(iter::once(Relation::name("document")
                        .attribute("id", Generator::id())
                        .attribute("content", Generator::text())))
    }

    fn generate_data(&self, db: &mut Database, relations: &[Relation]) {
        for i in 1..=self.0 {
            let mut insert = Insert::insert_into("document")
                .element("id", i)
                .element("content", format!("example {i}"));

            if let Some(gen) = relations.iter().find(|r| r.0 == "type").and_then(|r| r.1.get("id")) {
                insert = insert.element("type_id", gen.next());
            }

            db.insert(&insert).unwrap();
        }
    }
}

pub struct DocumentWithSize(pub Flavor);
impl Fixture for DocumentWithSize {
    fn generate_schema(&self) -> Box<dyn Iterator<Item = Relation>> {
        Box::new(iter::once(Relation::name("document")
                  .attribute("id", Generator::id())
                  .attribute("content", Generator::text())
                  .attribute("size", Generator::number_from(1))))
    }

    fn generate_data(&self, db: &mut Database, _: &[Relation]) {
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
    fn generate_schema(&self) -> Box<dyn Iterator<Item = Relation>> {
        let document = Relation::name("document")
                  .attribute("id", Generator::id())
                  .attribute("content", Generator::text())
                  .attribute("type_id", Generator::cycle(2));

        let doc_type = Relation::name("type")
            .attribute("id", Generator::id())
            .attribute("name", Generator::letter("type_"));

        Box::new([document, doc_type].into_iter())
    }

    fn generate_data(&self, db: &mut Database, _: &[Relation]) {
        db.insert(&Insert::insert_into("document").element("id", 1).element("content", "example").element("type_id", 2)).unwrap();
        db.insert(&Insert::insert_into("document").element("id", 2).element("content", "no type").element("type_id", 3)).unwrap();
        db.insert(&Insert::insert_into("type").element("id", 1).element("name", "type_a")).unwrap();
        db.insert(&Insert::insert_into("type").element("id", 2).element("name", "type_b")).unwrap();
    }
}

pub struct DocType(u32);

impl DocType {
    pub fn size(n: u32) -> Self {
        Self(n)
    }
}

impl Fixture for DocType {
    fn generate_schema(&self) -> Box<dyn Iterator<Item = Relation>> {
        Box::new(iter::once(Relation::name("type")
                  .attribute("id", Generator::id())
                  .attribute("name", Generator::letter("type_"))))
    }

    fn generate_data(&self, db: &mut Database, _: &[Relation]) {
        let chars = "abcdefgh";
        for (id, ch) in chars.char_indices().cycle().take(self.0 as usize) {
            db.insert(&Insert::insert_into("type")
                      .element("id", id)
                      .element("name", format!("type_{ch}"))).unwrap();
        }
    }

    fn before_define<'a>(&self, mut def: Relation) -> Relation {
        if def.0 == "document" {
            def = def.attribute("type_id", Generator::cycle(self.0));
        }

        def
    }
}

pub enum Flavor {
    SchemaOnly,
    Size(u32),
}

pub trait Fixture {
    fn generate_schema(&self) -> Box<dyn Iterator<Item = Relation>>;
    fn generate_data(&self, _: &mut Database, _: &[Relation]) {
    }


    fn before_define(&self, def: Relation) -> Relation {
        def
    }
}
