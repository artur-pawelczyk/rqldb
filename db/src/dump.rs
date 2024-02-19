use crate::{schema::Relation, Command, Query, RawObjectView, tuple::Tuple, dsl::AttrKind};

pub(crate) fn dump_create(rel: &Relation) -> Command {
    rel.columns().fold(Command::create_table(rel.name()), |acc, col| {
        if col.indexed() {
            acc.indexed_column(col.name(), col.kind())
        } else {
            acc.column(col.name(), col.kind())
        }
    })
}

pub(crate) fn dump_values<'a>(obj: &'a RawObjectView<'a>) -> QueryIter<'a> {
    QueryIter{ name: obj.name().to_string(),
               inner: obj.raw_tuples(),
    }
}

pub(crate) struct QueryIter<'a> {
    name: String,
    inner: Box<dyn Iterator<Item = Tuple<'a>> + 'a>,
}

impl<'a> Iterator for QueryIter<'a> {
    type Item = String;

    fn next(&mut self) -> Option<String> {
        self.inner.next().map(|tuple| {
            let vals: Vec<String> = tuple.iter().map(|cell| cell.to_string()).collect();
            Query::tuple(&vals.iter().map(|x| (AttrKind::Infer, x.as_str())).collect::<Vec<_>>())
                .insert_into(&self.name)
                .to_string()
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{schema::Schema, Type, Command, Database, Query};

    #[test]
    fn test_dump_schema() {
        let mut schema = Schema::default();
        schema.create_table("example")
            .indexed_column("id", Type::NUMBER)
            .column("content", Type::TEXT)
            .add();

        let rel = schema.find_relation("example").unwrap();

        assert_eq!(dump_create(rel), Command::create_table("example")
                   .indexed_column("id", Type::NUMBER)
                   .column("content", Type::TEXT));
    }

    #[test]
    fn test_dump_contents() {
        let mut db = Database::default();

        db.execute_create(&Command::create_table("example")
                          .indexed_column("id", Type::NUMBER)
                          .column("content", Type::TEXT));

        let creation_query = Query::tuple_untyped(&["1", "first"]).insert_into("example");
        db.execute_query(&creation_query).unwrap();

        let object = db.raw_object("example").unwrap();
        let first = dump_values(&object).next().unwrap();
        assert_eq!(first, creation_query.to_string());
    }
}
