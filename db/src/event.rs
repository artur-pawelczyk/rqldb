use std::error::Error;

use crate::{object::ObjectId, page::BlockId, schema::Relation, Database};

type EventResult = Result<(), Box<dyn Error>>;

pub trait EventSource {
    fn on_define_relation(&mut self, handler: impl Fn(&Self, &Relation) -> () + 'static);
    fn on_add_tuple(&mut self, handler: impl Fn(ObjectId, &[u8]) -> () + 'static);
    fn on_delete_tuple(&mut self, handler: impl Fn(u32) -> () + 'static);
    fn on_page_modified(&mut self, handler: impl Fn(ObjectId, BlockId, &[u8]) -> EventResult + 'static);
}

impl EventSource for Database {
    fn on_define_relation(&mut self, handler: impl Fn(&Self, &Relation) -> () + 'static) {
        self.handler.borrow_mut().on_def_relation = Some(Box::new(handler));
    }

    fn on_add_tuple(&mut self, handler: impl Fn(ObjectId, &[u8]) -> () + 'static) {
        self.handler.borrow_mut().on_add_tuple = Some(Box::new(handler));
    }

    fn on_delete_tuple(&mut self, handler: impl Fn(u32) -> () + 'static) {
        self.handler.borrow_mut().on_delete_tuple = Some(Box::new(handler));
    }

    fn on_page_modified(&mut self, handler: impl Fn(ObjectId, BlockId, &[u8]) -> EventResult + 'static) {
        self.handler.borrow_mut().on_page_modified = Some(Box::new(handler));
    }
}

#[derive(Default)]
pub(crate) struct EventHandler {
    on_def_relation: Option<Box<dyn Fn(&Database, &Relation) -> ()>>,
    on_add_tuple: Option<Box<dyn Fn(ObjectId, &[u8]) -> ()>>,
    on_delete_tuple: Option<Box<dyn Fn(u32) -> ()>>,
    on_page_modified: Option<Box<dyn Fn(ObjectId, BlockId, &[u8]) -> EventResult>>,
}

impl EventHandler {
    pub(crate) fn emit_define_relation(&self, db: &Database, rel: &Relation) {
        self.on_def_relation.as_ref().map(|h| h(db, rel));
    }

    pub(crate) fn emit_add_tuple(&self, obj: ObjectId, bytes: &[u8]) {
        self.on_add_tuple.as_ref().map(|h| h(obj, bytes));
    }

    pub(crate) fn emit_delete_tuple(&self, tid: u32) {
        self.on_delete_tuple.as_ref().map(|h| h(tid));
    }

    pub(crate) fn emit_page_modified(&self, obj: ObjectId, block: BlockId, page: &[u8]) {
        self.on_page_modified.as_ref().map(|h| {
            if let Err(e) = h(obj, block, page) {
                eprintln!("Error while emiting insert page event: {e}");
            }
        });
    }
}

#[cfg(test)]
mod tests {
    use std::{cell::{Cell, RefCell}, error::Error, rc::Rc};

    use super::*;

    use crate::{dsl::TupleBuilder, page::PAGE_SIZE, Database, Definition, Query, Type};

    #[test]
    fn event_on_define_relation() -> Result<(), Box<dyn Error>> {
        let mut db = Database::default();

        let created_relation = Rc::new(RefCell::new(String::new()));
        db.on_define_relation({
            let created_relation = Rc::clone(&created_relation);
            move |_, rel| { created_relation.borrow_mut().push_str(rel.name()); }
        });
        assert_eq!(created_relation.borrow().as_str(), "");

        db.define(&Definition::relation("document").attribute("id", Type::TEXT))?;

        assert_eq!(created_relation.borrow().as_str(), "document");

        Ok(())
    }

    #[test]
    fn event_on_add_tuple() -> Result<(), Box<dyn Error>> {
        let mut db = Database::default();
        db.define(&Definition::relation("document")
                  .attribute("id", Type::NUMBER)
                  .attribute("name", Type::TEXT))?;

        let created_tuple = Rc::new(RefCell::new(Vec::<u8>::new()));
        db.on_add_tuple({
            let created_tuple = Rc::clone(&created_tuple);
            move |_, b| { created_tuple.borrow_mut().extend(b); }
        });

        assert!(created_tuple.borrow().is_empty());

        db.insert(&Query::tuple(TupleBuilder::new()
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
                  .attribute("name", Type::TEXT))?;
        db.insert(&Query::tuple(TupleBuilder::new()
                                       .inferred("id", "1")
                                       .inferred("name", "example")).insert_into("document"))?;

        let deleted_id: Rc<Cell<Option<u32>>> = Rc::new(Cell::new(None));
        db.on_delete_tuple({
            let deleted_id = Rc::clone(&deleted_id);
            move |id| {
                deleted_id.set(Some(id));
            }
        });

        db.delete(&Query::scan("document").delete())?;

        assert!(deleted_id.get().is_some());

        Ok(())
    }

    #[test]
    fn event_on_insert_page() -> Result<(), Box<dyn Error>> {
        let mut db = Database::default();
        db.define(&Definition::relation("something")
                  .attribute("id", Type::NUMBER))?;
        db.define(&Definition::relation("document")
                  .attribute("id", Type::NUMBER)
                  .attribute("name", Type::TEXT))?;
        let document_obj_id = db.object("document").unwrap().borrow().id();

        let affected_obj = Rc::new(Cell::new(None::<ObjectId>));
        let inserted_page = Rc::new(RefCell::new(Vec::<u8>::new()));
        db.on_page_modified({
            let affected_obj = Rc::clone(&affected_obj);
            let inserted_page = Rc::clone(&inserted_page);
            move |obj, _, page| {
                affected_obj.set(Some(obj));
                inserted_page.borrow_mut().extend(page);
                Ok(())
            }
        });

        db.insert(&Query::tuple(TupleBuilder::new()
                                       .inferred("id", "1")
                                       .inferred("name", "example")).insert_into("document"))?;
       
        assert_eq!(affected_obj.get(), Some(document_obj_id));
        assert_eq!(inserted_page.borrow().len(), PAGE_SIZE);
        assert!(inserted_page.borrow()[0] != 0);

        Ok(())
    }
    #[test]
    fn event_page_modified_on_delete_tuple() -> Result<(), Box<dyn Error>> {
        let mut db = Database::default();
        db.define(&Definition::relation("document")
                  .attribute("id", Type::NUMBER)
                  .attribute("name", Type::TEXT))?;

        db.insert(&Query::tuple(TupleBuilder::new()
                                       .inferred("id", "1")
                                       .inferred("name", "example")).insert_into("document"))?;

        let called = Rc::new(Cell::new(false));
        db.on_page_modified({
            let called = Rc::clone(&called);
            move |_, _, _| { called.set(true); Ok(()) }
        });

        db.delete(&Query::scan("document").delete())?;

        assert!(called.get());

        Ok(())
    }
}
