use core::fmt;
use std::cell::{Cell, Ref, RefCell};
use std::collections::VecDeque;
use std::io;
use std::iter::zip;
use std::rc::Rc;

use crate::database::obj_view::ObjectView;
use crate::dump::{dump_values, dump_create};

use crate::event::EventHandler;
use crate::object::{Attribute, IndexedObject, ObjectId};
use crate::page::TupleId;
use crate::plan::{self, ApplyFn, Filter, Finisher, Mapper, Plan, Source};
use crate::schema::{AttributeRef, TableId};
use crate::tuple::Tuple;
use crate::{schema::Schema, QueryResults, plan::compute_plan};
use crate::{dsl, Query, ResultAttribute};

use super::{Error, Result, SharedObject};

#[derive(Default)]
pub struct Database {
    pub(crate) schema: Schema,
    pub(crate) objects: Vec<SharedObject>,
    pub(crate) handler: Rc<RefCell<EventHandler>>,
}

impl Database {
    pub fn with_schema(schema: Schema) -> Self {
        let handler = Rc::new(RefCell::new(EventHandler::default()));
        let objects = schema.relations.iter()
            .map(|rel| IndexedObject::from_table(rel).with_handler(Rc::clone(&handler)))
            .map(|o| Rc::new(RefCell::new(o)))
            .collect();

        Self {
            schema,
            objects,
            handler,
        }
    }

    pub fn execute_query(&self, query: &dsl::Query) -> Result<QueryResults> {
        self.execute_plan(compute_plan(self, query)?)
    }

    pub fn delete(&self, cmd: &dsl::Delete) -> Result<()> {
        let plan = compute_plan(&self, &cmd.0)?;
        let source = ObjectView::from(&plan.source);

        let ids = source.iter()
            .filter(|t| test_filters(&plan.filters, t))
            .map(|t| t.id())
            .collect::<Vec<_>>();

        drop(source);
        let mut object = match &plan.source {
            Source::TableScan(obj) => obj.borrow_mut(),
            Source::IndexScan(obj, _) => obj.borrow_mut(),
            _ => { return Err(format!("Cannot execute delete for this type of query").into()); }
        };

        object.remove_tuples(&ids);

        Ok(())
    }

    fn execute_plan(&self, plan: Plan) -> Result<QueryResults> {
        if plan.is_result_immediate() {
            return self.execute_plan_immediate(plan);
        }

        let ids = ObjectView::from(&plan.source).iter().map(|t| t.id()).collect::<VecDeque<_>>();

        let source = match &plan.source {
            Source::TableScan(obj) => Rc::clone(obj),
            Source::IndexScan(obj, _) => Rc::clone(obj),
            s => todo!("{}", s),
        };

        let attributes = plan.final_attributes().iter().map(Into::into).collect();
        let results = ResultIter {
            source,
            ids,
            joins: Box::from(plan.joins),
            filters: Box::from(plan.filters),
            mappers: Box::from(plan.mappers),
        };

        Ok(QueryResults {
            attributes,
            results: Cell::new(Box::new(results)),
        })
    }

    fn execute_plan_immediate(&self, plan: Plan) -> Result<QueryResults> {
        let source = ObjectView::from(&plan.source);
        let join_sources: Vec<Ref<IndexedObject>> = plan.joins.iter().map(|join| join.source_object().borrow()).collect();
        let mut sink = self.create_sink(&plan);

        for source_tuple in source.iter() {
            let mut tuple = source_tuple;
            let mut joined = None;

            for (join, source_object) in zip(plan.joins.iter(), join_sources.iter()) {
                joined = Some(false);
                let key = tuple.element(join.joinee_key());
                if let Some(join_source) = source_object.iter().find(|bytes| bytes.element(join.joiner_key()) == key) {
                    tuple = tuple.extend(join_source);
                    joined = Some(true);
                }
            }

            if !joined.unwrap_or(true) {
                continue;
            }

            if test_filters(&plan.filters, &tuple) {
                sink.accept_tuple(&tuple);
            }
        }

        Ok(sink.into_results())
    }

    fn create_sink(&self, plan: &Plan) -> Sink {
        match &plan.finisher {
            Finisher::Return => Sink::Return(plan.final_attributes(), vec![]),
            Finisher::Apply(f, attr) => match f {
                ApplyFn::Max => Sink::Max(*attr, 0),
                ApplyFn::Sum => Sink::Sum(*attr, 0),
                _ => Sink::NoOp,
            }
            Finisher::Count => Sink::Count(0),
            Finisher::Nil => Sink::NoOp,
        }
    }

    pub(crate) fn object(&self, id: impl TableId) -> Option<&SharedObject> {
        let rel = self.schema.find_relation(id)?;
        self.objects.get(rel.id as usize)
    }

    pub fn schema(&self) -> &Schema {
        &self.schema
    }

    pub fn read_object(&mut self, id: ObjectId, r: impl io::Read) -> Result<()> {
        let obj = self.objects.get(id as usize).ok_or_else(|| format!("No such object: {id}"))?;
        obj.borrow_mut().read(r).map_err(|_| format!("IO error"))?;
        Ok(())
    }

    pub fn dump(&self, name: &str, writer: &mut impl fmt::Write) -> Result<()> {
        let rel = self.schema.find_relation(name).unwrap();
        writeln!(writer, ".define {}", dump_create(rel)).map_err(|_| Error::from("Stdio error"))?;
        let result = self.execute_query(&Query::scan(name)).unwrap();
        dump_values(name, result, writer);

        Ok(())
    }

    pub fn dump_all(&self, writer: &mut impl fmt::Write) -> Result<()> {
        for rel in &self.schema.relations {
            self.dump(rel.name(), writer)?;
        }

        Ok(())
    }
}

fn test_filters(filters: &[Filter], tuple: &Tuple) -> bool {
    filters.iter().all(|filter| filter.matches_tuple(tuple))
}

#[derive(Debug)]
enum Sink {
    Count(usize),
    Sum(AttributeRef, i32),
    Max(AttributeRef, i32),
    Return(Vec<Attribute>, Vec<Vec<u8>>),
    NoOp,
}

impl Sink {
    fn accept_tuple(&mut self, tuple: &Tuple) {
        match self {
            Self::Count(ref mut count) => *count += 1,
            Self::Sum(attr, ref mut sum) => *sum += tuple.element(attr).unwrap().as_number().unwrap(),
            Self::Max(attr, ref mut max) => *max = std::cmp::max(*max, tuple.element(attr).unwrap().as_number().unwrap()),
            Self::Return(attrs, ref mut results) => results.push(tuple_to_cells(attrs, tuple)),
            Self::NoOp => {},
        }
    }

    fn into_results(self) -> QueryResults {
        match self {
            Self::Count(count) => QueryResults::single_number("count", count as i32),
            Self::Sum(_, n) => QueryResults::single_number("sum", n),
            Self::Max(_, n) => QueryResults::single_number("max", n),
            Self::Return(attributes, results) => {
                QueryResults {
                    attributes: attributes.iter().map(ResultAttribute::from).collect(),
                    results: Cell::new(Box::new(results.into_iter())),
                }
            }
            Self::NoOp => QueryResults::empty(),
        }
    }
}

fn tuple_to_cells(attrs: &[Attribute], tuple: &Tuple) -> Vec<u8> {
    use crate::object::NamedAttribute as _;

    attrs.iter().fold(Vec::new(), |mut bytes, attr| {
        if let Some(elem) = tuple.element(attr) {
            debug_assert!(attr.kind() == elem.kind());
            debug_assert!(attr.name() == elem.name());
            bytes.extend(elem.bytes());
        } else {
            panic!("Attribute {} not found in the tuple", attr.name());
        }

        bytes
    })
}

struct ResultIter {
    ids: VecDeque<TupleId>,
    source: SharedObject,
    joins: Box<[plan::Join]>,
    mappers: Box<[plan::Mapper]>,
    filters: Box<[plan::Filter]>,
}

impl ResultIter {
    fn by_id(&self, id: TupleId) -> Option<Vec<u8>> {
        let source = self.source.borrow();
        let tuple = source.get(id);
        let mut output = tuple.raw_bytes().to_vec();

        // TODO: Use mappers for joins
        for join in &self.joins {
            let key = tuple.element(join.joinee_key());
            if let Some(join_tuple) = join.source_object()
                .borrow().iter()
                .find(|join_tuple| join_tuple.element(join.joiner_key()) == key) {
                    output.extend(join_tuple.raw_bytes());
                } else {
                    return None;
                }
        }

        if test_filters(&self.filters, &tuple) {
            Some(output)
        } else {
            None
        }
    }
}

impl Iterator for ResultIter {
    type Item = Vec<u8>;

    fn next(&mut self) -> Option<Self::Item> {
        while let Some(id) = self.ids.pop_back() {
            if let Some(tuple) = self.by_id(id) {
                return Some(tuple);
            }
        }

        None
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use dsl::Insert;

    use super::*;
    use crate::{bytes::IntoBytes as _, dsl::{Definition, Operator, Query, TupleBuilder}, test::fixture::{Dataset, DocSize, DocType, Document}, EventSource, Type};

    #[test]
    fn query_not_existing_relation() {
        let query = Query::scan("not_real_relation").select_all();
        let db = Database::default();
        let result = db.execute_query(&query);
        assert!(result.is_err());
    }

    #[test]
    fn query_empty_relation() {
        let db = Dataset::default()
            .add(Document::empty())
            .generate(Database::default());

        let query = Query::scan("document").select_all();
        let result = db.execute_query(&query).unwrap();

        assert_eq!(result.tuples().count(), 0);
        let attrs = result.attributes();
        assert_eq!(
            attrs.iter().map(ResultAttribute::name).collect::<HashSet<_>>(),
            ["document.id", "document.content"].into()
        );
    }

    #[test]
    pub fn filter() {
        let db = Dataset::default()
            .add(Document::size(20))
            .generate(Database::default());

        let mut result = db.execute_query(&Query::scan("document").filter("document.id", Operator::EQ, "5")).unwrap();
        assert_eq!(result.tuples().count(), 1);

        result = db.execute_query(&Query::scan("document").filter("document.id", Operator::GT, "5").filter("document.id", Operator::LT, "10")).unwrap();
        assert_eq!(result.tuples().count(), 4);

        result = db.execute_query(&Query::scan("document").filter("document.content", Operator::EQ, "example 1")).unwrap();
        assert_eq!(result.tuples().count(), 1);

        assert!(db.execute_query(&Query::scan("document").filter("not_a_field", Operator::EQ, "1")).is_err());
    }

    #[test]
    fn single_join() {
        let db = Dataset::default()
            .add(Document::size(2))
            .add(DocType::size(2))
            .generate(Database::default());

        let result = db.execute_query(&Query::scan("document").join("document.type_id", "type.id")).unwrap();
        assert_eq!(
            result.attributes.iter().map(ResultAttribute::name).collect::<HashSet<_>>(),
            ["document.id", "document.content", "document.type_id", "type.id", "type.name"].into()
        );
        let tuple = result.tuples().next().unwrap();
        let document_id = tuple.element("document.id").unwrap().to_string();
        let type_name = tuple.element("type.name").unwrap().to_string();
        assert_eq!(document_id, "1");
        assert_eq!(type_name, "type_b");
    }

    #[test]
    fn two_joins() {
        let mut db = Database::default();
        db.define(&Definition::relation("document")
                  .indexed_attribute("id", Type::NUMBER)
                  .attribute("type_id", Type::NUMBER)
                  .attribute("author", Type::TEXT)
        ).unwrap();

        db.define(&Definition::relation("type")
                  .indexed_attribute("id", Type::NUMBER)
                  .attribute("name", Type::TEXT)
        ).unwrap();

        db.define(&Definition::relation("author")
                  .indexed_attribute("name", Type::TEXT)
                  .attribute("displayname", Type::TEXT)
        ).unwrap();

        db.insert(&Query::tuple(&[("id", "1"), ("type_id", "1"), ("author", "admin")]).insert_into("document")).unwrap();
        db.insert(&Query::tuple(&[("id", "1"), ("name", "page")]).insert_into("type")).unwrap();
        db.insert(&Query::tuple(&[("name", "admin"), ("displayname", "the author")]).insert_into("author")).unwrap();

        let result = db.execute_query(&Query::scan("document").join("document.type_id", "type.id").join("document.author", "author.name")).unwrap();
        let tuple = result.tuples().next().unwrap();
        assert_eq!(tuple.element("author.displayname").unwrap().to_string(), "the author");
    }

    #[test]
    fn apply() {
        let db = Dataset::default()
            .add(Document::size(10))
            .add(DocSize)
            .db();

        let sum_result = db.execute_query(&Query::scan("document").apply("sum", &["document.size"])).unwrap();
        let first = sum_result.tuples().next().unwrap();
        let sum = first.element("sum")
            .and_then(|e| <i32>::try_from(e).ok())
            .unwrap();
        assert_eq!(sum, 55);

        let max_result = db.execute_query(&Query::scan("document").apply("max", &["document.size"])).unwrap();
        let first = max_result.tuples().next().unwrap();
        let max = first.element("max")
            .and_then(|e| <i32>::try_from(e).ok())
            .unwrap();
        assert_eq!(max, 10);
    }

    #[test]
    #[ignore]
    fn mapper() {
        let db = Dataset::default()
            .add(Document::size(1))
            .db();

        let result = db.execute_query(&Query::scan("document").set("document.content", "content after map")).unwrap();
        let mut tuples = result.tuples();
        assert_eq!(tuples.next().unwrap().element("document.content").unwrap().to_string(), "content after map");
    }

    #[test]
    fn count() {
        let db = Dataset::default()
            .add(Document::size(20))
            .db();

        let result = db.execute_query(&Query::scan("document").count()).unwrap();
        let first = result.tuples().next().unwrap();
        let count = first.element("count")
            .and_then(|e| <i32>::try_from(e).ok())
            .unwrap();
        assert_eq!(count, 20);
    }

    #[test]
    fn delete() {
        let db = Dataset::default()
            .add(Document::size(1))
            .db();

        let tuple_delete = db.delete(&Query::build_tuple().typed(Type::NUMBER, "id", 1).build().delete());
        assert!(tuple_delete.is_err());

        let no_such_table = db.delete(&Query::scan("something").delete());
        assert!(no_such_table.is_err());

        db.delete(&Query::scan("document").delete()).unwrap();

        let result = db.execute_query(&Query::scan("document")).unwrap();
        assert!(result.tuples().next().is_none());
    }

    #[test]
    fn duplicates() {
        let db = Dataset::default()
            .add(Document::empty())
            .db();

        let insert = Insert::insert_into("document").element("id", 1).element("content", "the content");
        db.insert(&insert).unwrap();
        db.insert(&insert).unwrap();

        let count_query = Query::scan("document").count();
        let result = db.execute_query(&count_query).unwrap();
        let first = result.tuples().next().unwrap();
        let count = first.element("count").unwrap();
        assert_eq!(count.try_into(), Ok(1));
    }

    #[test]
    fn recover_object() {
        let mut source_db = Database::default();
        source_db.define(&Definition::relation("document").attribute("id", Type::NUMBER).attribute("content", Type::TEXT)).unwrap();
        let contents = Rc::new(RefCell::new(Vec::<u8>::new()));
        source_db.on_page_modified({
            let contents = Rc::clone(&contents);
            move |_, _, bytes| {
                contents.borrow_mut().extend(bytes);
                Ok(())
            }
        });
        source_db.insert(&Query::tuple(TupleBuilder::new()
                                              .inferred("id", "1")
                                              .inferred("content", "name")).insert_into("document")).unwrap();

        let mut target_db = Database::default();
        target_db.define(&Definition::relation("document").attribute("id", Type::NUMBER).attribute("content", Type::TEXT)).unwrap();
        let object = target_db.object("document").unwrap().borrow().id();
        target_db.read_object(object, contents.borrow().as_slice()).unwrap();

        let all = target_db.execute_query(&Query::scan("document")).unwrap();
        assert_eq!(all.tuples().count(), 1);
    }

    #[test]
    fn index_scan() {
        let mut db = Database::default();
        db.define(&Definition::relation("document").indexed_attribute("id", Type::NUMBER).attribute("content", Type::TEXT)).unwrap();

        for id in 1..20 {
            let content = format!("example{}", id);
            let query = Query::tuple(TupleBuilder::new()
                                     .inferred("id", id)
                                     .inferred("content", content)
            ).insert_into("document");
            db.insert(&query).expect("Insert");
        }

        let result = db.execute_query(&Query::scan_index("document.id", Operator::EQ, "5")).unwrap();
        let tuple_found = result.tuples().next().unwrap();
        assert_eq!(tuple_found.element("document.id").unwrap().try_into(), Ok(5i32));
        assert_eq!(tuple_found.element("document.content").unwrap().as_bytes(), &"example5".to_byte_vec());

        let tuple_not_found = db.execute_query(&Query::scan_index("document.id", Operator::EQ, "500")).unwrap();
        assert_eq!(tuple_not_found.tuples().count(), 0);

        let missing_table = db.execute_query(&Query::scan_index("notable.id", Operator::EQ, "a"));
        assert!(missing_table.is_err());

        let missing_index = db.execute_query(&Query::scan_index("document.content", Operator::EQ, "a"));
        assert!(missing_index.is_err());
    }

    #[test]
    fn dump_all() {
        let mut db = Database::default();
        db.define(&Definition::relation("first").indexed_attribute("id", Type::NUMBER).attribute("content", Type::TEXT)).unwrap();
        db.define(&Definition::relation("second").attribute("num", Type::NUMBER)).unwrap();
        db.insert(&Query::tuple(&[("id", "1"), ("content", "one")]).insert_into("first")).unwrap();

        let expected = concat!(
            ".define relation first id::NUMBER::KEY content::TEXT\n",
            ".insert first first.content = one first.id = 1\n",
            ".define relation second num::NUMBER\n",
        );

        let mut out = String::new();
        db.dump_all(&mut out).unwrap();
        assert_eq!(out, expected);
    }
}
