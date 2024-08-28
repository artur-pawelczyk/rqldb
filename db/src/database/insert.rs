use crate::bytes::write_as_bytes;
use crate::object::NamedAttribute as _;
use crate::{dsl, Database};
use crate::database::Result;

impl Database {
    pub fn insert(&self, cmd: &dsl::Insert<'_, Vec<dsl::TupleAttr<'_>>>) -> Result<()> {
        let mut target = self.object(cmd.target)
            .ok_or_else(|| format!("Relation {} not found", cmd.target))?
            .borrow_mut();

        let mut byte_tuple = Vec::new();
        for attr in target.attributes() {
            let elem = cmd.tuple
                .iter().find(|elem| elem.name == attr.name.as_ref() || elem.name == attr.short_name())
                .ok_or_else(|| format!("Attribute {} is missing", attr.name))?;

            write_as_bytes(attr.kind, &elem.value, &mut byte_tuple)
                .map_err(|_| format!("Error encoding value for attribute {}", attr.name))?;
        }

        target.add_tuple(&byte_tuple);

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use dsl::{Definition, Query};

    use crate::Type;

    use super::*;

    #[test]
    pub fn insert() {
        let mut db = Database::default();

        let command = Definition::relation("document")
            .attribute("id", Type::NUMBER)
            .attribute("content", Type::TEXT);
        db.define(&command);

        let insert_query = Query::build_tuple()
            .inferred("id", "1")
            .inferred("content", "something")
            .build().insert_into("document");
        let insert_result = db.insert(&insert_query);
        assert!(insert_result.is_ok());

        let query = Query::scan("document").select_all();
        let result = db.execute_query(&query).unwrap();
        let tuples: Vec<_> = result.tuples().collect();
        assert_eq!(tuples.len(), 1);
        let tuple = tuples.first().expect("fail");
        assert_eq!(<i32>::try_from(tuple.element("document.id").unwrap()), Ok(1));
        assert_eq!(tuple.element("document.content").unwrap().to_string(), "something");
    }

    #[test]
    pub fn failed_insert() {
        let mut db = Database::default();

        let command = Definition::relation("document")
            .attribute("id", Type::NUMBER)
            .attribute("content", Type::TEXT);
        db.define(&command);

        let result = db.insert(&Query::tuple(&[("id", "not-a-number"), ("id", "random-text")]).insert_into("document"));
        assert!(result.is_err());
    }

    #[test]
    fn update() {
        let mut db = Database::default();
        db.define(&Definition::relation("document").indexed_attribute("id", Type::NUMBER).attribute("content", Type::TEXT));

        db.insert(&Query::build_tuple()
                  .inferred("id", "1")
                  .inferred("content", "original content")
                  .build().insert_into("document")).unwrap();
        db.insert(&Query::build_tuple()
                  .inferred("id", "1")
                  .inferred("content", "new content")
                  .build().insert_into("document")).unwrap();

        let result = db.execute_query(&Query::scan("document")).unwrap();
        let mut tuples = result.tuples();
        assert_eq!(tuples.next().unwrap().element("document.content").unwrap().to_string(), "new content");
        assert!(tuples.next().is_none());
    }
}