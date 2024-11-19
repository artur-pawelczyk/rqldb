use std::{collections::BTreeMap, iter};

use crate::{dsl::{Insert, TupleBuilder}, Database, Definition};

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

        for f in &self.0 {
            f.generate_data(&mut db, &relations);
        }

        db
    }

    pub fn db(self) -> Database {
        self.generate(Default::default())
    }
}

pub struct Relation {
    name: &'static str,
    attributes: BTreeMap<&'static str, Generator>,
    index: Option<&'static str>,
}

impl Relation {
    fn name(name: &'static str) -> Self {
        Self {
            name,
            attributes: BTreeMap::new(),
            index: None,
        }
    }

    fn attribute(mut self, name: &'static str, gen: Generator) -> Self {
        self.attributes.insert(name, gen);
        self
    }

    fn index(mut self, attr: &'static str) -> Self {
        self.index = Some(attr);
        self
    }
}

impl From<&Relation> for Definition {
    fn from(rel: &Relation) -> Self {
        let mut def = Definition::relation(rel.name);
        for (name, gen) in &rel.attributes {
            if Some(name) == rel.index.as_ref() {
                def = def.indexed_attribute(name, gen.kind());
            } else {
                def = def.attribute(name, gen.kind());
            }
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
        for _ in 1..=self.0 {
            let mut tuple = TupleBuilder::new();
            for (name, gen) in &relations.iter().find(|r| r.name == "document").unwrap().attributes {
                tuple = tuple.inferred(name, gen.next());
            }

            db.insert(&Insert::insert_into("document").tuple(tuple)).unwrap();
        }
    }
}

pub struct DocSize;
impl Fixture for DocSize {
    fn generate_schema(&self) -> Box<dyn Iterator<Item = Relation>> {
        Box::new(iter::empty())
    }

    fn before_define(&self, mut def: Relation) -> Relation {
        if def.name == "document" {
            def = def.attribute("size", Generator::number_from(1));
        }

        def
    }
}

pub struct DocIdIndex;
impl Fixture for DocIdIndex {
    fn generate_schema(&self) -> Box<dyn Iterator<Item = Relation>> {
        Box::new(iter::empty())
    }

    fn before_define(&self, mut def: Relation) -> Relation {
        if def.name == "document" {
            def = def.index("id");
        }

        def
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
                      .element("id", id + 1)
                      .element("name", format!("type_{ch}"))).unwrap();
        }
    }

    fn before_define<'a>(&self, mut def: Relation) -> Relation {
        if def.name == "document" {
            def = def.attribute("type_id", Generator::cycle(self.0));
        }

        def
    }
}

pub struct Index(&'static str, &'static str);
impl Fixture for Index {
    fn generate_schema(&self) -> Box<dyn Iterator<Item = Relation>> {
        Box::new(iter::empty())
    }

    fn before_define(&self, mut def: Relation) -> Relation {
        if def.name == self.0 {
            def.index = Some(self.1);
        }

        def
    }
}

pub trait Fixture {
    fn generate_schema(&self) -> Box<dyn Iterator<Item = Relation>>;
    fn generate_data(&self, _: &mut Database, _: &[Relation]) {
    }


    fn before_define(&self, def: Relation) -> Relation {
        def
    }
}
