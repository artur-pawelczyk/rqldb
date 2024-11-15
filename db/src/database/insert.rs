use std::io::Write;

use crate::bytes::write_as_bytes;
use crate::object::{NamedAttribute, PartialTuple, TupleAddError};
use crate::{dsl, Database, Query};
use crate::database::Result;

use super::Error;

impl Database {
    pub fn insert<'a, T: Into<Query<'a>> + Clone + 'a>(&self, cmd: &dsl::Insert<T>) -> Result<()> {
        let mut target = self.object(cmd.target)
            .ok_or_else(|| format!("Relation {} not found", cmd.target))?
            .borrow_mut();
        let query: Query = cmd.tuple.clone().into();

        match &query.source {
            dsl::Source::Tuple(values) => {
                target.add_tuple(&values.as_slice())
                    .map_err(Error::from)
            },
            _ => Err(Error("Unsupported query type for insert".into())),
        }
    }
}

impl PartialTuple for &dsl::Insert<'_> {
    fn write_element_at<A, W>(&self, attr: &A, out: W) -> bool
    where A: NamedAttribute,
    W: Write
    {
        self.tuple.as_slice().write_element_at(attr, out)
    }
}

impl PartialTuple for &[dsl::TupleAttr<'_>] {
    fn write_element_at<A, W>(&self, attr: &A, mut out: W) -> bool
    where A: NamedAttribute,
          W: Write {
        self.iter()
            .find(|elem| elem.name == attr.name() || elem.name == attr.short_name())
            .map(|elem| write_as_bytes(attr.kind(), &elem.value, &mut out).is_ok() )
            .unwrap_or(false)
    }
}

impl From<TupleAddError> for Error {
    fn from(value: TupleAddError) -> Self {
        Self(value.msg)
    }
}

#[cfg(test)]
mod tests {
    use dsl::{Definition, Operator, Query};

    use crate::{test::fixture::{self, Dataset}, Type};

    use super::*;

    #[test]
    pub fn insert() {
        let mut db = Database::default();

        let command = Definition::relation("document")
            .attribute("id", Type::NUMBER)
            .attribute("content", Type::TEXT);
        db.define(&command).unwrap();

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
        db.define(&command).unwrap();

        let result = db.insert(&Query::tuple(&[("id", "not-a-number"), ("id", "random-text")]).insert_into("document"));
        assert!(result.is_err());
    }

    #[test]
    fn update() {
        let mut db = Database::default();
        db.define(&Definition::relation("document").indexed_attribute("id", Type::NUMBER).attribute("content", Type::TEXT)).unwrap();

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

    #[test]
    fn update_with_index() -> std::result::Result<(), Box<dyn std::error::Error>> {
        let db = Dataset::default()
            .add(fixture::Document::size(1))
            .add(fixture::DocIdIndex)
            .add(fixture::DocSize)
            .generate(Database::default());

        db.insert(&Query::build_tuple()
                  .inferred("id", 1)
                  .inferred("content", "updated content")
                  .inferred("size", 100)
                  .build().insert_into("document")).unwrap();

        let result = db.execute_query(&Query::scan_index("document.id", Operator::EQ, "1"))?;
        let mut tuples = result.tuples();
        assert_eq!(tuples.next().unwrap().element("document.content").unwrap().to_string(), "updated content");

        Ok(())
    }
}
