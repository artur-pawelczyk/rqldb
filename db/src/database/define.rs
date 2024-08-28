use std::{cell::RefCell, rc::Rc};

use crate::{object::IndexedObject, Definition};

use super::{Database, Result};

impl Database {
    pub fn define(&mut self, definition: &Definition) -> Result<()> {
        let relation = definition.columns.iter().fold(self.schema.create_table(&definition.name), |acc, col| {
            if col.indexed {
                acc.indexed_column(&col.name, col.kind)
            } else {
                acc.column(&col.name, col.kind)
            }
        }).add().ok_or_else(|| format!("Relation {} already exists", definition.name))?;

        self.objects.insert(
            relation.id as usize,
            Rc::new(RefCell::new(IndexedObject::from_table(relation).with_handler(Rc::clone(&self.handler))))
        );

        let rel_id = relation.id;
        self.schema().find_relation(rel_id).map(|r| self.handler.borrow().emit_define_relation(&self, r));

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{test::fixture::{Document, Fixture, Flavor}, Type};

    #[test]
    fn redefine_non_empty_relation() {
        let mut db = Database::default().generate_dataset(Document(Flavor::Size(1)));

        let result = db.define(&Definition::relation("document").attribute("id", Type::NUMBER));
        assert!(result.is_err());
    }
}
