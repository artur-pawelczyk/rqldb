use crate::{schema::Relation, Database};

pub trait EventSource {
    fn on_define_relation(&mut self, handler: impl Fn(&Relation) -> () + 'static);
}

impl EventSource for Database {
    fn on_define_relation(&mut self, handler: impl Fn(&Relation) -> () + 'static) {
        self.handler.on_def_relation = Some(Box::new(handler));
    }
}

#[derive(Default)]
pub(crate) struct EventHandler {
    on_def_relation: Option<Box<dyn Fn(&Relation) -> ()>>,
}

impl EventHandler {
    pub(crate) fn emit_define_relation(&self, rel: &Relation) {
        self.on_def_relation.as_ref().map(|h| h(rel));
    }
}

#[cfg(test)]
mod tests {
    use std::{cell::RefCell, rc::Rc};

    use super::*;

    use crate::{Database, Definition, Type};

    #[test]
    fn event_on_define_relation() {
        let mut db = Database::default();

        let created_relation = Rc::new(RefCell::new(String::new()));
        db.on_define_relation({
            let created_relation = Rc::clone(&created_relation);
            move |rel| { created_relation.borrow_mut().push_str(rel.name()); }
        });
        assert_eq!(created_relation.borrow().as_str(), "");

        db.define(&Definition::relation("document").attribute("id", Type::TEXT));

        assert_eq!(created_relation.borrow().as_str(), "document");
    }
}
