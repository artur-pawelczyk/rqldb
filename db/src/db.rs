use core::fmt;
use std::cell::{RefCell, Ref, RefMut};
use std::rc::Rc;

use crate::dump::{dump_values, dump_create};
use crate::object::{IndexedObject, TempObject, Attribute};
use crate::plan::{Source, Filter, Plan, Finisher, ApplyFn};
use crate::schema::{AttributeRef, TableId};
use crate::tuple::Tuple;
use crate::{schema::Schema, QueryResults, plan::compute_plan};
use crate::{dsl, Query, RawObjectView, ResultAttribute};

type Result<T> = std::result::Result<T, String>;

pub(crate) type SharedObject = Rc<RefCell<IndexedObject>>;

#[derive(Default)]
pub struct Database {
    schema: Schema,
    objects: Vec<SharedObject>,
}

impl Database {
    pub fn with_schema(schema: Schema) -> Self {
        let objects = schema.relations.iter()
            .map(IndexedObject::from_table)
            .map(|o| Rc::new(RefCell::new(o)))
            .collect();

        Self{
            schema,
            objects,
        }
    }

    pub fn define(&mut self, definition: &dsl::Definition) {
        let table = definition.columns.iter().fold(self.schema.create_table(&definition.name), |acc, col| {
            if col.indexed {
                acc.indexed_column(&col.name, col.kind)
            } else {
                acc.column(&col.name, col.kind)
            }
        }).add();

        self.objects.insert(table.id, Rc::new(RefCell::new(IndexedObject::from_table(table))));
    }

    pub fn execute_query(&self, query: &dsl::Query) -> Result<QueryResults> {
        self.execute_plan(compute_plan(self, query)?)
    }

    fn execute_plan(&self, plan: Plan) -> Result<QueryResults> {
        let source: ObjectView = match &plan.source {
            Source::TableScan(obj) => {
                ObjectView::Ref(obj.borrow())
            },
            Source::Tuple(values_map) => {
                let attrs = plan.source.attributes();
                let mut temp_object = TempObject::from_attrs(&attrs);
                let values: Vec<_> = values_map.values().collect();
                temp_object.push_str(&values);
                ObjectView::Val(temp_object)
            },
            Source::ReferencedTuple(obj, ref map) => {
                let temp_object = TempObject::from_object(&obj.borrow());
                let mut tuple = temp_object.build_tuple();
                for (k, v) in map {
                    tuple.add(k, v);
                }
                ObjectView::Val(tuple.build())
            },
            Source::IndexScan(obj, val) => {
                if let Some(tuple_id) = obj.borrow().find_in_index(val) {
                    ObjectView::TupleRef(obj.borrow(), tuple_id)
                } else {
                    ObjectView::Empty
                }
            },
            _ => unimplemented!()
        };

        let join_sources: Vec<Ref<IndexedObject>> = plan.joins.iter().map(|join| join.source_object().borrow()).collect();
        let mut sink = self.create_sink(&plan);

        for (idx, byte_tuple) in source.iter().enumerate() {
            let tuple = match &plan.joins[..] {
                [] => byte_tuple,
                [join] => {
                    let joinee = byte_tuple;
                    let key = joinee.element(join.joinee_key());
                    let source_object = join_sources.first().expect("join source is computed from the list of joins");
                    if let Some(join_source) = source_object.iter().find(|bytes| bytes.element(join.joiner_key()) == key) {
                        joinee.extend(join_source)
                    } else {
                        joinee
                    }
                },
                _ => return Err(String::from("Multiple joins aren't supported"))
            };

            if test_filters(&plan.filters, &tuple) {
                sink.accept_tuple(idx, &tuple);
            }
        }

        drop(source);
        if let Finisher::Delete(obj) = &plan.finisher {
            sink.accept_object(obj.borrow_mut());
        }

        Ok(sink.into_results())
    }

    fn create_sink<'a>(&'a self, plan: &'a Plan) -> Sink<'a> {
        match &plan.finisher {
            Finisher::Return => Sink::Return(plan.final_attributes(), vec![]),
            Finisher::Apply(f, attr) => match f {
                ApplyFn::Max => Sink::Max(*attr, 0),
                ApplyFn::Sum => Sink::Sum(*attr, 0),
                _ => Sink::NoOp,
            }
            Finisher::Count => Sink::Count(0),
            Finisher::Insert(obj) => Sink::Insert(obj.borrow_mut()),
            Finisher::Delete(_) => Sink::Delete(vec![]),
            Finisher::Nil => Sink::NoOp,
        }
    }

    pub(crate) fn object(&self, id: impl TableId) -> Option<&SharedObject> {
        let rel = self.schema.find_relation(id)?;
        self.objects.get(rel.id)
    }

    pub fn raw_object<'a>(&'a self, name: &str) -> Option<RawObjectView<'a>> {
        let rel = self.schema.find_relation(name)?;
        let o = self.objects.get(rel.id)?;
        o.borrow_mut().vaccum();
        Some(RawObjectView{ rel, object: o.borrow() })
    }

    pub fn raw_objects(&self) -> Vec<RawObjectView> {
        self.schema.relations.iter().flat_map(|rel| self.raw_object(rel.name())).collect()
    }

    pub fn schema(&self) -> &Schema {
        &self.schema
    }

    pub fn recover_object(&mut self, id: usize, snapshot: TempObject) {
        let table = self.schema.find_relation(id).unwrap();
        let new_obj = IndexedObject::recover(snapshot, table);
        let _ = std::mem::replace(&mut self.objects[id], Rc::new(RefCell::new(new_obj)));
    }

    pub fn dump(&self, name: &str, writer: &mut impl fmt::Write) -> Result<()> {
        let rel = self.schema.find_relation(name).unwrap();
        writeln!(writer, ".define {}", dump_create(rel)).map_err(|_| "Stdio error")?;
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

enum ObjectView<'a> {
    Ref(Ref<'a, IndexedObject>),
    TupleRef(Ref<'a, IndexedObject>, usize),
    Val(TempObject),
    Empty,
}

impl<'a> ObjectView<'a> {
    fn iter(&'a self) -> Box<dyn Iterator<Item = Tuple<'a>> + 'a> {
        match self {
            Self::Ref(o) => o.iter(),
            Self::TupleRef(o, id) => Box::new(std::iter::once_with(|| o.get(*id).unwrap())),
            Self::Val(o) => o.iter(),
            Self::Empty => Box::new(std::iter::empty()),
        }
    }
}

#[derive(Debug)]
enum Sink<'a> {
    Count(usize),
    Sum(AttributeRef, i32),
    Max(AttributeRef, i32),
    Return(Vec<Attribute>, Vec<Vec<u8>>),
    Insert(RefMut<'a, IndexedObject>),
    Delete(Vec<usize>),
    NoOp,
}

impl<'a> Sink<'a> {
    fn accept_tuple(&mut self, idx: usize, tuple: &Tuple) {
        match self {
            Self::Count(ref mut count) => *count += 1,
            Self::Sum(attr, ref mut sum) => *sum += tuple.element(attr).unwrap().as_number().unwrap(),
            Self::Max(attr, ref mut max) => *max = std::cmp::max(*max, tuple.element(attr).unwrap().as_number().unwrap()),
            Self::Return(attrs, ref mut results) => results.push(tuple_to_cells(attrs, tuple)),
            Self::Insert(object) => { object.add_tuple(tuple); },
            Self::Delete(ref mut tuples) => tuples.push(idx),
            Self::NoOp => {},
        }
    }

    fn accept_object(&mut self, mut object: RefMut<IndexedObject>) {
        if let Self::Delete(ids) = self {
            object.remove_tuples(ids);
        }
    }

    fn into_results(self) -> QueryResults {
        match self {
            Self::Count(count) => QueryResults::single_number("count", count as i32),
            Self::Sum(_, n) => QueryResults::single_number("sum", n),
            Self::Max(_, n) => QueryResults::single_number("max", n),
            Self::Return(attributes, results) => {
                QueryResults{
                    attributes: attributes.iter().map(ResultAttribute::from).collect(),
                    results,
                }
            }
            Self::Insert(_) => QueryResults::empty(),
            Self::Delete(_) => QueryResults::empty(),
            Self::NoOp => QueryResults::empty(),
        }
    }
}

fn tuple_to_cells(attrs: &[Attribute], tuple: &Tuple) -> Vec<u8> {
    attrs.iter().fold(Vec::new(), |mut bytes, attr| {
        if let Some(elem) = tuple.element(attr) {
            bytes.extend(elem.bytes());
        }

        bytes
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{bytes::IntoBytes as _, dsl::{Definition, Operator, Query, TupleBuilder}, Type};

    #[test]
    fn query_not_existing_relation() {
        let query = Query::scan("not_real_relation").select_all();
        let db = Database::default();
        let result = db.execute_query(&query);
        assert!(result.is_err());
    }

    #[test]
    fn query_empty_relation() {
        let mut db = Database::default();
        let command = Definition::relation("document")
            .attribute("id", Type::NUMBER)
            .attribute("content", Type::TEXT);

        db.define(&command);

        let query = Query::scan("document").select_all();
        let result = db.execute_query(&query).unwrap();

        assert_eq!(result.tuples().count(), 0);
        let attrs = result.attributes();
        assert_eq!(
            attrs.iter().map(ResultAttribute::name).collect::<Vec<_>>(),
            vec!["document.id", "document.content"]
        );
    }

    #[test]
    pub fn insert() {
        let mut db = Database::default();

        let command = Definition::relation("document")
            .attribute("id", Type::NUMBER)
            .attribute("content", Type::TEXT);
        db.define(&command);

        let insert_query = Query::tuple(&[("id", "1"), ("content", "something")]).insert_into("document");
        let insert_result = db.execute_query(&insert_query);
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

        let result = db.execute_query(&Query::tuple(&[("id", "not-a-number"), ("id", "random-text")]).insert_into("document"));
        assert!(result.is_err());
    }

    #[test]
    pub fn filter() {
        let mut db = Database::default();
        db.define(&Definition::relation("document").attribute("id", Type::NUMBER).attribute("content", Type::TEXT));

        for i in 1..20 {
            let id = i.to_string();
            let content = format!("example{}", i);
            let query = Query::tuple(TupleBuilder::new().inferred("id", &id).inferred("content", &content)).insert_into("document");
            db.execute_query(&query).expect("Insert");
        }

        let mut result = db.execute_query(&Query::scan("document").filter("document.id", Operator::EQ, "5")).unwrap();
        assert_eq!(result.tuples().count(), 1);

        result = db.execute_query(&Query::scan("document").filter("document.id", Operator::GT, "5").filter("document.id", Operator::LT, "10")).unwrap();
        assert_eq!(result.tuples().count(), 4);

        result = db.execute_query(&Query::scan("document").filter("document.content", Operator::EQ, "example1")).unwrap();
        assert_eq!(result.tuples().count(), 1);

        assert!(db.execute_query(&Query::scan("document").filter("not_a_field", Operator::EQ, "1")).is_err());
    }

    #[test]
    pub fn join() {
        let mut db = Database::default();
        db.define(&Definition::relation("document").attribute("id", Type::NUMBER).attribute("content", Type::TEXT).attribute("type_id", Type::NUMBER));
        db.define(&Definition::relation("type").attribute("id", Type::NUMBER).attribute("name", Type::TEXT));

        db.execute_query(&Query::tuple(&[("id", "1"), ("content", "example"), ("type_id", "2")]).insert_into("document")).unwrap();
        db.execute_query(&Query::tuple(&[("id", "1"), ("name", "type_a")]).insert_into("type")).unwrap();
        db.execute_query(&Query::tuple(&[("id", "2"), ("name", "type_b")]).insert_into("type")).unwrap();

        let result = db.execute_query(&Query::scan("document").join("type", "document.type_id", "type.id")).unwrap();
        assert_eq!(
            result.attributes.iter().map(ResultAttribute::name).collect::<Vec<_>>(),
            vec!["document.id", "document.content", "document.type_id", "type.id", "type.name"]
        );
        let tuple = result.tuples().next().unwrap();
        let document_id = tuple.element("document.id").unwrap().to_string();
        let type_name = tuple.element("type.name").unwrap().to_string();
        assert_eq!(document_id, "1");
        assert_eq!(type_name, "type_b");
    }

    #[test]
    fn apply() {
        let mut db = Database::default();
        db.define(&Definition::relation("document")
                          .attribute("id", Type::NUMBER)
                          .attribute("content", Type::TEXT)
                          .attribute("size", Type::NUMBER));

        for i in 1..=10 {
            let s = i.to_string();
            let query = Query::tuple(TupleBuilder::new()
                                     .inferred("id", &s)
                                     .inferred("content", "example")
                                     .inferred("size", &s)
            ).insert_into("document");
           db.execute_query(&query).expect("Insert");
        }

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
    fn count() {
        let mut db = Database::default();
        db.define(&Definition::relation("document").attribute("id", Type::NUMBER).attribute("content", Type::TEXT));

        for i in 1..21 {
            let s = i.to_string();
            let query = Query::tuple(TupleBuilder::new()
                                     .inferred("id", &s)
                                     .inferred("content", "example")
            ).insert_into("document");
            db.execute_query(&query).expect("Insert");
        }

        let result = db.execute_query(&Query::scan("document").count()).unwrap();
        let first = result.tuples().next().unwrap();
        let count = first.element("count")
            .and_then(|e| <i32>::try_from(e).ok())
            .unwrap();
        assert_eq!(count, 20);
    }

    #[test]
    fn delete() {
        let mut db = Database::default();
        db.define(&Definition::relation("document").attribute("id", Type::NUMBER).attribute("content", Type::TEXT));
        db.execute_query(&Query::tuple(&[("id", "1"), ("content", "the content")]).insert_into("document")).unwrap();

        let tuple_delete = db.execute_query(&Query::tuple(&[("id", "1")]).delete());
        assert!(tuple_delete.is_err());

        let no_such_table = db.execute_query(&Query::scan("something").delete());
        assert!(no_such_table.is_err());

        db.execute_query(&Query::scan("document").delete()).unwrap();

        let result = db.execute_query(&Query::scan("document")).unwrap();
        assert!(result.tuples().next().is_none());
    }

    #[test]
    fn duplicates() {
        let mut db = Database::default();
        db.define(&Definition::relation("document").attribute("id", Type::NUMBER).attribute("content", Type::TEXT));

        let insert_query = Query::tuple(&[("id", "1"), ("content", "the content")]).insert_into("document");
        db.execute_query(&insert_query).unwrap();
        db.execute_query(&insert_query).unwrap();

        let count_query = Query::scan("document").count();
        let result = db.execute_query(&count_query).unwrap();
        let first = result.tuples().next().unwrap();
        let count = first.element("count").unwrap();
        assert_eq!(count.try_into(), Ok(1));
    }

    #[test]
    fn update() {
        let mut db = Database::default();
        db.define(&Definition::relation("document").indexed_attribute("id", Type::NUMBER).attribute("content", Type::TEXT));
        let obj = db.object("document").unwrap();

        db.execute_plan(Plan::insert(obj, &["1", "orig content"])).unwrap();
        db.execute_plan(Plan::insert(obj, &["1", "new content"])).unwrap();

        let result = db.execute_plan(Plan::scan(obj)).unwrap();
        let mut tuples = result.tuples();
        assert_eq!(tuples.next().unwrap().element("document.content").unwrap().to_string(), "new content");
        assert!(tuples.next().is_none());
    }

    #[test]
    fn recover_object() {
        let mut source_db = Database::default();
        source_db.define(&Definition::relation("document").attribute("id", Type::NUMBER).attribute("content", Type::TEXT));
        let source_obj = source_db.object("document").unwrap();

        let mut target_db = Database::default();
        target_db.define(&Definition::relation("document").attribute("id", Type::NUMBER).attribute("content", Type::TEXT));
        let target_obj = target_db.object("document").unwrap();

        source_db.execute_plan(Plan::insert(source_obj, &["1", "one"])).unwrap();
        let raw_object = source_db.raw_object("document").unwrap();
        let mut temp_object = TempObject::from_object(&target_obj.borrow());
        for tuple in raw_object.raw_tuples() {
            temp_object.push(tuple);
        }

        target_db.recover_object(0, temp_object);
        let target_obj = target_db.object("document").unwrap();
        let all = target_db.execute_plan(Plan::scan(target_obj)).unwrap();
        assert_eq!(all.tuples().count(), 1);
    }

    #[test]
    fn index_scan() {
        let mut db = Database::default();
        db.define(&Definition::relation("document").indexed_attribute("id", Type::NUMBER).attribute("content", Type::TEXT));

        for i in 1..20 {
            let id = i.to_string();
            let content = format!("example{}", i);
            let query = Query::tuple(TupleBuilder::new()
                                     .inferred("id", &id)
                                     .inferred("content", &content)
            ).insert_into("document");
            db.execute_query(&query).expect("Insert");
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
        db.define(&Definition::relation("first").indexed_attribute("id", Type::NUMBER).attribute("content", Type::TEXT));
        db.define(&Definition::relation("second").attribute("num", Type::NUMBER));
        db.execute_query(&Query::tuple(&[("id", "1"), ("content", "one")]).insert_into("first")).unwrap();

        let expected = concat!(
            ".define relation first id::NUMBER::KEY content::TEXT\n",
            "tuple first.content = one first.id = 1 | insert_into first\n",
            ".define relation second num::NUMBER\n",
        );

        let mut out = String::new();
        db.dump_all(&mut out).unwrap();
        assert_eq!(out, expected);
    }
}
