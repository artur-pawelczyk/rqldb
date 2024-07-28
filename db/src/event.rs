use crate::{schema::Relation, Database};

pub trait EventSource {
    fn on_define_relation(&mut self, handler: impl Fn(&Self, &Relation) -> () + 'static);
    fn on_add_tuple(&mut self, handler: impl Fn(usize, &[u8]) -> () + 'static);
    fn on_delete_tuple(&mut self, handler: impl Fn(u32) -> () + 'static);
}

impl EventSource for Database {
    fn on_define_relation(&mut self, handler: impl Fn(&Self, &Relation) -> () + 'static) {
        self.handler.borrow_mut().on_def_relation = Some(Box::new(handler));
    }

    fn on_add_tuple(&mut self, handler: impl Fn(usize, &[u8]) -> () + 'static) {
        self.handler.borrow_mut().on_add_tuple = Some(Box::new(handler));
    }

    fn on_delete_tuple(&mut self, handler: impl Fn(u32) -> () + 'static) {
        self.handler.borrow_mut().on_delete_tuple = Some(Box::new(handler));
    }
}

#[derive(Default)]
pub(crate) struct EventHandler {
    on_def_relation: Option<Box<dyn Fn(&Database, &Relation) -> ()>>,
    on_add_tuple: Option<Box<dyn Fn(usize, &[u8]) -> ()>>,
    on_delete_tuple: Option<Box<dyn Fn(u32) -> ()>>,
}

impl EventHandler {
    pub(crate) fn emit_define_relation(&self, db: &Database, rel: &Relation) {
        self.on_def_relation.as_ref().map(|h| h(db, rel));
    }

    pub(crate) fn emit_add_tuple(&self, obj: usize, bytes: &[u8]) {
        self.on_add_tuple.as_ref().map(|h| h(obj, bytes));
    }

    pub(crate) fn emit_delete_tuple(&self, tid: u32) {
        self.on_delete_tuple.as_ref().map(|h| h(tid));
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

        let created_tuple = Rc::new(RefCell::new(Vec::<u8>::new()));
        db.on_add_tuple({
            let created_tuple = Rc::clone(&created_tuple);
            move |_, b| { created_tuple.borrow_mut().extend(b); }
        });

        assert!(created_tuple.borrow().is_empty());

        db.execute_query(&Query::tuple(TupleBuilder::new()
                                       .inferred("id", "1")
                                       .inferred("name", "example")).insert_into("document"))?;

        assert!(!created_tuple.borrow().is_empty());
        Ok(())
    }

    #[test]
    fn event_on_delete_tuple() -> Result<(), Box<dyn Error>> {
        let mut db = Database::default();
        db.define(&Definition::relation("document")
                  .attribute("id", Type::NUMBER)
                  .attribute("name", Type::TEXT));
        db.execute_query(&Query::tuple(TupleBuilder::new()
                                       .inferred("id", "1")
                                       .inferred("name", "example")).insert_into("document"))?;

        let deleted_id: Rc<Cell<Option<u32>>> = Rc::new(Cell::new(None));
        db.on_delete_tuple({
            let deleted_id = Rc::clone(&deleted_id);
            move |id| {
                deleted_id.set(Some(id));
            }
        });

        db.execute_query(&Query::scan("document").delete())?;

        assert!(deleted_id.get().is_some());

        Ok(())
    }
}
