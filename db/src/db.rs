use std::cell::{RefCell, Ref, RefMut};

use crate::dump::{dump_values, dump_create};
use crate::object::IndexedObject;
use crate::plan::{Contents, Attribute, Filter, Plan, Finisher, ApplyFn};
use crate::tuple::Tuple;
use crate::{schema::Schema, QueryResults, plan::compute_plan};
use crate::{dsl, Cell, RawObjectView, Type};

#[derive(Default)]
pub struct Database<'a> {
    schema: Schema,
    objects: Vec<RefCell<IndexedObject<'a>>>,
}

type ByteCell = Vec<u8>;
type ByteTuple = Vec<ByteCell>;

impl<'obj> Database<'obj> {
    pub fn with_schema(schema: Schema) -> Self {
        let objects = schema.relations.iter().map(IndexedObject::from_table).map(RefCell::new).collect();

        Self{
            schema,
            objects,
        }
    }

    pub fn execute_create(&mut self, command: &dsl::Command) {
        let table = command.columns.iter().fold(self.schema.create_table(&command.name), |acc, col| {
            if col.indexed {
                acc.indexed_column(&col.name, col.kind)
            } else {
                acc.column(&col.name, col.kind)
            }
        }).add();

        self.objects.insert(table.id, RefCell::new(IndexedObject::from_table(table)));
    }

    pub fn execute_query(&self, query: &dsl::Query) -> Result<QueryResults, &'static str> {
        self.execute_plan(compute_plan(&self.schema, query)?)
    }

    fn execute_plan(&self, plan: Plan) -> Result<QueryResults, &'static str> {
        fn cell(s: &str) -> Vec<u8> {
            if let Ok(num) = s.parse::<i32>() {
                Vec::from(num.to_be_bytes())
            } else {
                Vec::from(s.as_bytes())
            }
        }

        let source: ObjectView = match plan.source.contents {
            Contents::TableScan(rel) => {
                ObjectView::Ref(self.objects.get(rel.id).expect("Already checked by the planner").borrow())
            },
            Contents::Tuple(ref values) => {
                let cells = values.iter().map(|val| cell(val)).collect();
                ObjectView::Val(IndexedObject::temporary(cells))
            },
            Contents::IndexScan(ref col, ref val) => {
                let object = self.objects.get(col.table().id).expect("Already checked by the planner").borrow();
                if let Some(tuple_id) = object.find_in_index(val.as_bytes()) {
                    ObjectView::TupleRef(object, tuple_id)
                } else {
                    ObjectView::Empty
                }
            },
            _ => unimplemented!()
        };

        let join_sources: Vec<Ref<IndexedObject>> = plan.joins.iter().map(|join| self.objects.get(join.source_table().id).unwrap().borrow()).collect();
        let mut sink = self.create_sink(&plan);

        for (idx, byte_tuple) in source.iter().enumerate() {
            let tuple = match &plan.joins[..] {
                [] => Tuple::from_bytes(byte_tuple),
                [join] => {
                    let joinee = Tuple::from_bytes(byte_tuple);
                    let key = joinee.cell_by_attr(join.joinee_key());
                    if let Some(found) = join_sources.get(0).unwrap().iter().find(|bytes| Tuple::from_bytes(bytes).cell_by_attr(join.joiner_key()) == key) {
                        joinee.add_cells(found)
                    } else {
                        joinee
                    }
                },
                _ => return Err("Multiple joins aren't supported")
            };

            if !source.is_removed(idx) && test_filters(&plan.filters, &tuple) {
                sink.accept_tuple(idx, &tuple);
            }
        }

        drop(source);
        if let Finisher::Delete(rel) = plan.finisher {
            sink.accept_object(self.objects.get(rel.id).unwrap().borrow_mut());
        }

        Ok(sink.into_results())
    }

    fn create_sink<'a>(&'a self, plan: &Plan) -> Sink<'a, 'obj> {
        match &plan.finisher {
            Finisher::Return => Sink::Return(plan.final_attributes(), vec![]),
            Finisher::Apply(f) => match f {
                ApplyFn::Max => Sink::Max(0),
                ApplyFn::Sum => Sink::Sum(0),
                _ => Sink::NoOp,
            }
            Finisher::Count => Sink::Count(0),
            Finisher::Insert(rel) => Sink::Insert(self.objects.get(rel.id).unwrap().borrow_mut()),
            Finisher::Delete(_) => Sink::Delete(vec![]),
            Finisher::Nil => Sink::NoOp,
        }
    }

    pub fn raw_object<'a>(&'a self, name: &str) -> Option<RawObjectView<'a>> {
        let rel = self.schema.find_relation(name)?;
        let o = self.objects.get(rel.id)?;
        Some(RawObjectView{ rel, object: o.borrow() })
    }

    pub fn raw_objects(&self) -> Vec<RawObjectView> {
        self.schema.relations.iter().flat_map(|rel| self.raw_object(rel.name())).collect()
    }

    pub fn schema(&self) -> &Schema {
        &self.schema
    }

    pub fn recover_object(&mut self, id: usize, snapshot: Vec<ByteTuple>) {
        let table = self.schema.find_relation(id).unwrap();
        let new_obj = IndexedObject::recover(snapshot, table);
        let _ = std::mem::replace(&mut self.objects[id], RefCell::new(new_obj));
    }

    pub fn dump(&self, name: &str) -> String {
        let mut out = String::new();
        let rel = self.schema.find_relation(name).unwrap();
        out.push_str(&dump_create(rel).to_string());
        out.push('\n');
        for tuple in dump_values(&self.raw_object(name).unwrap()) {
            out += &tuple;
            out.push('\n');
        }

        out
    }

    pub fn dump_all(&self) -> String {
        let mut out = String::new();
        for rel in &self.schema.relations {
            out += &self.dump(rel.name());
        }

        out.pop();
        out
    }
}

fn test_filters(filters: &[Filter], tuple: &Tuple) -> bool {
    filters.iter().all(|filter| filter.matches_tuple(tuple))
}

enum ObjectView<'a> {
    Ref(Ref<'a, IndexedObject<'a>>),
    TupleRef(Ref<'a, IndexedObject<'a>>, usize),
    Val(IndexedObject<'a>),
    Empty,
}

impl<'a> ObjectView<'a> {
    fn iter(&'a self) -> Box<dyn Iterator<Item = &'a ByteTuple> + 'a> {
        match self {
            Self::Ref(o) => o.iter(),
            Self::TupleRef(o, id) => Box::new(std::iter::once_with(|| o.get(*id).unwrap())),
            Self::Val(o) => o.iter(),
            Self::Empty => Box::new(std::iter::empty()),
        }
    }

    fn is_removed(&self, id: usize) -> bool {
        match self {
            Self::Ref(object) => object.is_removed(id),
            _ => false,
        }
    }
}

#[derive(Debug)]
enum Sink<'a, 'obj> {
    Count(usize),
    Sum(i32),
    Max(i32),
    Return(Vec<Attribute>, Vec<Vec<Cell>>),
    Insert(RefMut<'a, IndexedObject<'obj>>),
    Delete(Vec<usize>),
    NoOp,
}

impl<'a, 'obj> Sink<'a, 'obj> {
    fn accept_tuple(&mut self, idx: usize, tuple: &Tuple) {
        match self {
            Self::Count(ref mut count) => *count += 1,
            Self::Sum(ref mut sum) => *sum += tuple.cell(1, Type::NUMBER).unwrap().as_number().unwrap(),
            Self::Max(ref mut max) => *max = std::cmp::max(*max, tuple.cell(1, Type::NUMBER).unwrap().as_number().unwrap()),
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
            Self::Count(count) => QueryResults::count(count as u32),
            // TODO: "count" doesn't belong here
            Self::Sum(n) => QueryResults::count(n as u32),
            Self::Max(n) => QueryResults::count(n as u32),
            Self::Return(attributes, results) => {
                QueryResults{
                    attributes: attributes.into_iter().map(|attr| attr.into_name()).collect(),
                    results,
                }
            }
            Self::Insert(_) => QueryResults::empty(),
            Self::Delete(_) => QueryResults::empty(),
            Self::NoOp => QueryResults::empty(),
        }
    }
}

fn tuple_to_cells(attrs: &[Attribute], tuple: &Tuple) -> Vec<Cell> {
    attrs.iter().map(|attr| Cell::from_bytes(attr.kind(), tuple.cell_by_attr(attr).bytes())).collect()
}


#[cfg(test)]
mod tests {
    use super::*;
    use crate::{dsl::{Command, Operator, Query}, Type};

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
        let command = Command::create_table("document")
            .column("id", Type::NUMBER)
            .column("content", Type::TEXT);

        db.execute_create(&command);

        let query = Query::scan("document").select_all();
        let result = db.execute_query(&query);

        assert!(result.is_ok());
        let tuples = result.unwrap();
        assert_eq!(tuples.size(), 0);
        let attrs = tuples.attributes();
        assert_eq!(attrs.as_slice(), ["document.id".to_string(), "document.content".to_string()]);
    }

    #[test]
    pub fn insert() {
        let mut db = Database::default();

        let command = Command::create_table("document")
            .column("id", Type::NUMBER)
            .column("content", Type::TEXT);
        db.execute_create(&command);

        let insert_query = Query::tuple(&["1", "something"]).insert_into("document");
        let insert_result = db.execute_query(&insert_query);
        assert!(insert_result.is_ok());

        let query = Query::scan("document").select_all();
        let result = db.execute_query(&query);
        assert!(result.is_ok());
        let tuples = result.unwrap();
        assert_eq!(tuples.size(), 1);
        let results = tuples.results();
        let tuple = results.get(0).expect("fail");
        assert_eq!(tuple.contents[0].as_number(), Some(1i32));
        assert_eq!(&tuple.contents[1].as_string(), "something");
    }

    #[test]
    pub fn failed_insert() {
        let mut db = Database::default();

        let command = Command::create_table("document")
            .column("id", Type::NUMBER)
            .column("content", Type::TEXT);
        db.execute_create(&command);

        let result = db.execute_query(&Query::tuple(&["not-a-number", "random-text"]).insert_into("document"));
        assert!(result.is_err());
    }

    #[test]
    pub fn filter() {
        let mut db = Database::default();
        db.execute_create(&Command::create_table("document").column("id", Type::NUMBER).column("content", Type::TEXT));

        for i in 1..20 {
            let content = format!("example{}", i);
            db.execute_query(&Query::tuple(&[i.to_string().as_str(), content.as_str()]).insert_into("document")).expect("Insert");
        }

        let mut result = db.execute_query(&Query::scan("document").filter("document.id", Operator::EQ, "5")).unwrap();
        assert_eq!(result.size(), 1);

        result = db.execute_query(&Query::scan("document").filter("document.id", Operator::GT, "5").filter("document.id", Operator::LT, "10")).unwrap();
        assert_eq!(result.size(), 4);

        result = db.execute_query(&Query::scan("document").filter("document.content", Operator::EQ, "example1")).unwrap();
        assert_eq!(result.size(), 1);

        assert!(db.execute_query(&Query::scan("document").filter("not_a_field", Operator::EQ, "1")).is_err());
    }

    #[test]
    pub fn join() {
        let mut db = Database::default();
        db.execute_create(&Command::create_table("document").column("id", Type::NUMBER).column("content", Type::TEXT).column("type_id", Type::NUMBER));
        db.execute_create(&Command::create_table("type").column("id", Type::NUMBER).column("name", Type::TEXT));

        db.execute_query(&Query::tuple(&["1", "example", "2"]).insert_into("document")).unwrap();
        db.execute_query(&Query::tuple(&["1", "type_a"]).insert_into("type")).unwrap();
        db.execute_query(&Query::tuple(&["2", "type_b"]).insert_into("type")).unwrap();

        let result = db.execute_query(&Query::scan("document").join("type", "document.type_id", "type.id")).unwrap();
        assert_eq!(*result.attributes, ["document.id", "document.content", "document.type_id", "type.id", "type.name"]);
        let tuple = result.tuple_at(0).unwrap();
        let document_id = tuple.cell_by_name("document.id").unwrap().as_string();
        let type_name = tuple.cell_by_name("type.name").unwrap().as_string();
        assert_eq!(document_id, "1");
        assert_eq!(type_name, "type_b");
    }

    #[test]
    fn apply() {
        let mut db = Database::default();
        db.execute_create(&Command::create_table("document").column("id", Type::NUMBER).column("size", Type::NUMBER));
        for i in 1..=10 {
            db.execute_query(&Query::tuple(&[i.to_string().as_str(), i.to_string().as_str()]).insert_into("document")).expect("Insert");
        }

        let sum_result = db.execute_query(&Query::scan("document").apply("sum", &["document.size"])).unwrap();
        let sum = sum_result.results().get(0)
            .and_then(|t| t.cell_at(0))
            .and_then(|c| c.as_number())
            .unwrap();
        assert_eq!(sum, 55);

        let max_result = db.execute_query(&Query::scan("document").apply("max", &["document.size"])).unwrap();
        let max = max_result.results().get(0)
            .and_then(|t| t.cell_at(0))
            .and_then(|c| c.as_number())
            .unwrap();
        assert_eq!(max, 10);
    }

    #[test]
    fn count() {
        let mut db = Database::default();
        db.execute_create(&Command::create_table("document").column("id", Type::NUMBER).column("content", Type::TEXT));

        for i in 1..21 {
            db.execute_query(&Query::tuple(&[i.to_string().as_str(), "example"]).insert_into("document")).expect("Insert");
        }

        let result = db.execute_query(&Query::scan("document").count()).unwrap();
        let count = result.results().get(0)
            .and_then(|t| t.cell_at(0))
            .and_then(|c| c.as_number())
            .unwrap();
        assert_eq!(count, 20);
    }

    #[test]
    fn delete() {
        let mut db = Database::default();
        db.execute_create(&Command::create_table("document").column("id", Type::NUMBER).column("content", Type::TEXT));
        db.execute_query(&Query::tuple(&["1", "the content"]).insert_into("document")).unwrap();

        let tuple_delete = db.execute_query(&Query::tuple(&["1"]).delete());
        assert!(tuple_delete.is_err());

        let no_such_table = db.execute_query(&Query::scan("something").delete());
        assert!(no_such_table.is_err());

        db.execute_query(&Query::scan("document").delete()).unwrap();

        let result = db.execute_query(&Query::scan("document")).unwrap();
        assert!(result.results().is_empty());
    }

    #[test]
    fn duplicates() {
        let mut db = Database::default();
        db.execute_create(&Command::create_table("document").column("id", Type::NUMBER).column("content", Type::TEXT));

        let insert_query = Query::tuple(&["1", "the content"]).insert_into("document");
        db.execute_query(&insert_query).unwrap();
        db.execute_query(&insert_query).unwrap();

        let count_query = Query::scan("document").count();
        let result = db.execute_query(&count_query).unwrap();
        assert_eq!(result.results().get(0).unwrap().cell_by_name("count").unwrap().as_number(), Some(1));
    }

    #[test]
    fn update() {
        let mut db = Database::default();
        db.execute_create(&Command::create_table("document").indexed_column("id", Type::NUMBER).column("content", Type::TEXT));
        let table = db.schema.find_relation("document").unwrap();

        db.execute_plan(Plan::insert(table, &["1".to_string(), "orig content".to_string()])).unwrap();
        db.execute_plan(Plan::insert(table, &["1".to_string(), "new content".to_string()])).unwrap();

        let result = db.execute_plan(Plan::scan(table)).unwrap();
        assert_eq!(result.results().get(0).unwrap().cell_by_name("document.content").unwrap().as_string(), "new content");
        assert_eq!(result.size(), 1);
    }

    #[test]
    fn recover_object() {
        let mut db = Database::default();
        db.execute_create(&Command::create_table("document").column("id", Type::NUMBER).column("content", Type::TEXT));
        let table = db.schema.find_relation("document").unwrap().clone();

        db.execute_plan(Plan::insert(&table, &["1".to_string(), "one".to_string()])).unwrap();
        let snapshot = db.raw_object("document").unwrap().raw_tuples().cloned().collect();

        db.execute_plan(Plan::insert(&table, &["2".to_string(), "two".to_string()])).unwrap();
        let all = db.execute_plan(Plan::scan(&table)).unwrap();
        assert_eq!(all.size(), 2);

        db.recover_object(0, snapshot);
        let all = db.execute_plan(Plan::scan(&table)).unwrap();
        assert_eq!(all.size(), 1);
    }

    #[test]
    fn index_scan() {
        let mut db = Database::default();
        db.execute_create(&Command::create_table("document").indexed_column("id", Type::NUMBER).column("content", Type::TEXT));

        for i in 1..20 {
            let content = format!("example{}", i);
            db.execute_query(&Query::tuple(&[i.to_string().as_str(), content.as_str()]).insert_into("document")).expect("Insert");
        }

        let tuple_found = db.execute_query(&Query::scan_index("document.id", Operator::EQ, "5")).unwrap();
        assert_eq!(tuple_found.results()[0].cell_at(0), Some(&Cell::from_number(5)));
        assert_eq!(tuple_found.results()[0].cell_at(1), Some(&Cell::from_string("example5")));

        let tuple_not_found = db.execute_query(&Query::scan_index("document.id", Operator::EQ, "500")).unwrap();
        assert_eq!(tuple_not_found.size(), 0);

        let missing_table = db.execute_query(&Query::scan_index("notable.id", Operator::EQ, "a"));
        assert!(missing_table.is_err());

        let missing_index = db.execute_query(&Query::scan_index("document.content", Operator::EQ, "a"));
        assert!(missing_index.is_err());
    }

    #[test]
    fn dump_all() {
        let mut db = Database::default();
        db.execute_create(&Command::create_table("first").indexed_column("id", Type::NUMBER).column("content", Type::TEXT));
        db.execute_create(&Command::create_table("second").column("num", Type::NUMBER));
        db.execute_query(&Query::tuple(&["1", "one"]).insert_into("first")).unwrap();

        let expected = concat!(
            "create_table first id::NUMBER::KEY content::TEXT\n",
            "tuple 1 one | insert_into first\n",
            "create_table second num::NUMBER",
        );

        assert_eq!(db.dump_all(), expected);
    }
}
