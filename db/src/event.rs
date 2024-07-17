use crate::{schema::Relation, Database};

pub trait EventSource {
    fn on_define_relation(&mut self, handler: impl Fn(&Self, &Relation) -> () + 'static);
    fn on_add_tuple(&mut self, handler: impl Fn() -> () + 'static);
}

impl EventSource for Database {
    fn on_define_relation(&mut self, handler: impl Fn(&Self, &Relation) -> () + 'static) {
        self.handler.borrow_mut().on_def_relation = Some(Box::new(handler));
    }

    fn on_add_tuple(&mut self, handler: impl Fn() -> () + 'static) {
        self.handler.borrow_mut().on_add_tuple = Some(Box::new(handler));
    }
}

#[derive(Default)]
pub(crate) struct EventHandler {
    on_def_relation: Option<Box<dyn Fn(&Database, &Relation) -> ()>>,
    on_add_tuple: Option<Box<dyn Fn() -> ()>>,
}

impl EventHandler {
    pub(crate) fn emit_define_relation(&self, db: &Database, rel: &Relation) {
        self.on_def_relation.as_ref().map(|h| h(db, rel));
    }

    pub(crate) fn emit_add_tuple(&self) {
        self.on_add_tuple.as_ref().map(|h| h());
    }
}

#[cfg(test)]
mod tests {
    use std::{cell::{Cell, RefCell}, error::Error, rc::Rc};

    use super::*;

    use crate::{dsl::TupleBuilder, Database, Definition, Query, Type};

    #[test]
    fn event_on_define_relation() {
        let mut db = Database::default();

        let created_relation = Rc::new(RefCell::new(String::new()));
        db.on_define_relation({
            let created_relation = Rc::clone(&created_relation);
            move |_, rel| { created_relation.borrow_mut().push_str(rel.name()); }
        });
        assert_eq!(created_relation.borrow().as_str(), "");

        db.define(&Definition::relation("document").attribute("id", Type::TEXT));

        assert_eq!(created_relation.borrow().as_str(), "document");
    }

    #[test]
    fn event_on_add_tuple() -> Result<(), Box<dyn Error>> {
        let mut db = Database::default();
        db.define(&Definition::relation("document")
                  .attribute("id", Type::NUMBER)
                  .attribute("name", Type::TEXT));

        let created_tuple = Rc::new(Cell::new(false));
        db.on_add_tuple({
            let created_tuple = Rc::clone(&created_tuple);
            move || { created_tuple.set(true); }
        });

        assert_eq!(created_tuple.get(), false);

        db.execute_query(&Query::tuple(TupleBuilder::new()
                                       .inferred("id", "1")
                                       .inferred("name", "example")).insert_into("document"))?;

        assert_eq!(created_tuple.get(), true);
        Ok(())
    }
}
