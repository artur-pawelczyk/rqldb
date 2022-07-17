use std::cell::{RefCell, Ref, RefMut};
use std::collections::HashSet;

use crate::index::{Index, Op};
use crate::plan::{Contents, Attribute, Filter, Plan, Finisher};
use crate::schema::Column;
use crate::tuple::Tuple;
use crate::{schema::Schema, QueryResults, plan::compute_plan};
use crate::{dsl, Cell, Type};

#[derive(Default)]
pub struct Database {
    schema: Schema,
    objects: Vec<RefCell<Object>>,
}

struct Object {
    tuples: Vec<ByteTuple>,
    removed_ids: HashSet<usize>,
    index: Index,
}

type ByteTuple = Vec<Vec<u8>>;

impl Object {
    fn temporary(byte_tuple: ByteTuple) -> Self {
        let mut obj = Self::default();
        obj.add_tuple(&Tuple::from_bytes(&byte_tuple));
        obj
    }

    fn from_columns(columns: &[Column]) -> Self {
        let indexed_column = columns.iter().enumerate().find(|(_, col)| col.indexed).map(|(i, _)| i);
        Self{
            tuples: Vec::new(),
            removed_ids: HashSet::new(),
            index: indexed_column.map(|i| Index::single_cell(&[], i)).unwrap_or_else(|| Index::new(&[])),
        }
    }

    fn recover(&self, snapshot: Vec<ByteTuple>) -> Self {
        let index = self.index.rebuild(&snapshot);
        Self{
            tuples: snapshot,
            removed_ids: HashSet::new(),
            index
        }
    }

    fn add_tuple(&mut self, tuple: &Tuple) -> bool {
        match self.index.on(&self.tuples).insert(tuple.as_bytes()) {
            Op::Insert(id) => {
                self.tuples.insert(id, tuple.as_bytes().clone());
                return true;
            }
            Op::Replace(id) => {
                let _ = std::mem::replace(&mut self.tuples[id], tuple.as_bytes().clone());
                return true;
            }
        }

    }

    fn iter(&self) -> std::slice::Iter<ByteTuple> {
        self.tuples.iter()
    }

    fn remove_tuples(&mut self, ids: &[usize]) {
        self.removed_ids.extend(ids);
    }
}

impl Default for Object {
    fn default() -> Self {
        Object{
            tuples: vec![],
            removed_ids: HashSet::new(),
            index: Index::new(&[]),
        }
    }
}

impl Database {
    pub fn execute_create(&mut self, command: &dsl::Command) {
        command.columns.iter().fold(self.create_table(&command.name), |acc, col| {
            if col.indexed {
                acc.indexed_column(&col.name, col.kind)
            } else {
                acc.column(&col.name, col.kind)
            }
        }).create();
    }

    #[must_use]
    pub fn create_table(&mut self, name: &str) -> TableCreation {
        TableCreation{ db: self, name: name.to_string(), columns: Vec::new() }
    }

    fn create_table_impl(&mut self, name: String, columns: Vec<Column>) {
        self.schema.add_relation(self.objects.len(), &name, &columns);
        self.objects.push(RefCell::new(Object::from_columns(&columns)));
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
                ObjectView::Ref(self.objects.get(rel.id).unwrap().borrow())
            },
            Contents::Tuple(ref values) => {
                let cells = values.iter().map(|val| cell(val)).collect();
                ObjectView::Val(Object::temporary(cells))
            },
            _ => todo!()
        };

        let join_sources: Vec<Ref<Object>> = plan.joins.iter().map(|join| self.objects.get(join.source_table().id).unwrap().borrow()).collect();
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
                _ => todo!(),
            };

            if !source.is_removed(idx) && test_filters(&plan.filters, &tuple) {
                sink.accept_tuple(idx, &tuple);
            }
        }

        drop(source);
        match plan.finisher {
            Finisher::Delete(rel) => sink.accept_object(self.objects.get(rel.id).unwrap().borrow_mut()),
            _ => {},
        }

        Ok(sink.into_results())
    }



    fn create_sink<'a>(&'a self, plan: &'a Plan) -> Sink<'a> {
        match plan.finisher {
            Finisher::Return => Sink::Return(plan.final_attributes(), vec![]),
            Finisher::Count => Sink::Count(0),
            Finisher::Insert(rel) => Sink::Insert(self.objects.get(rel.id).unwrap().borrow_mut()),
            Finisher::Delete(_) => Sink::Delete(vec![]),
            Finisher::Nil => Sink::NoOp,
        }
    }

    pub fn raw_object(&self, name: &str) -> Option<RawObjectView> {
        let rel = self.schema.find_relation(name)?;
        let o = self.objects.get(rel.id)?;
        Some(RawObjectView{ object: o.borrow() })
    }

    pub fn raw_objects(&self) -> Vec<RawObjectView> {
        self.objects.iter().map(|o| RawObjectView{ object: o.borrow() }).collect()
    }

    pub fn schema(&self) -> &Schema {
        &self.schema
    }

    pub fn recover_object(&self, id: usize, snapshot: Vec<ByteTuple>) {
        let mut obj = self.objects.get(id).unwrap().borrow_mut();
        let new_obj = obj.recover(snapshot);
        *obj = new_obj;
    }

    pub fn print_statistics(&self) {
        for rel in &self.schema.relations {
            let obj = self.objects.get(rel.id).unwrap().borrow();
            println!("table {} index statistics", rel.name);
            obj.index.print_statistics();
        }
    }
}

pub struct TableCreation<'a> {
    db: &'a mut Database,
    name: String,
    columns: Vec<Column>,
}

impl<'a> TableCreation<'a> {
    #[must_use]
    pub fn column(mut self, name: &str, kind: Type) -> Self {
        self.columns.push(Column{ name: name.to_string(), kind, indexed: false });
        self
    }

    #[must_use]
    pub fn indexed_column(mut self, name: &str, kind: Type) -> Self {
        self.columns.push(Column{ name: name.to_string(), kind, indexed: true });
        self
    }

    pub fn create(self) {
        self.db.create_table_impl(self.name, self.columns);
    }
}

fn test_filters(filters: &[Filter], tuple: &Tuple) -> bool {
    filters.iter().all(|filter| filter.matches_tuple(tuple))
}

enum ObjectView<'a> {
    Ref(Ref<'a, Object>),
    Val(Object),
}

impl<'a> AsRef<Object> for ObjectView<'a> {
    fn as_ref(&self) -> &Object {
        match self {
            Self::Ref(o) => o,
            Self::Val(o) => o,
        }
    }
}

impl<'a> ObjectView<'a> {
    fn iter(&self) -> std::slice::Iter<ByteTuple> {
        match self {
            Self::Ref(o) => o.iter(),
            Self::Val(o) => o.iter(),
        }
    }

    fn is_removed(&self, id: usize) -> bool {
        match self {
            Self::Ref(object) => object.removed_ids.contains(&id),
            Self::Val(_) => false,
        }
    }
}

enum Sink<'a> {
    Count(usize),
    Return(Vec<Attribute>, Vec<Vec<Cell>>),
    Insert(RefMut<'a, Object>),
    Delete(Vec<usize>),
    NoOp,
}

impl<'a> Sink<'a> {
    fn accept_tuple(&mut self, idx: usize, tuple: &Tuple) {
        match self {
            Self::Count(ref mut count) => *count += 1,
            Self::Return(attrs, ref mut results) => results.push(tuple_to_cells(attrs, tuple)),
            Self::Insert(object) => { object.add_tuple(tuple); },
            Self::Delete(ref mut tuples) => tuples.push(idx),
            Self::NoOp => {},
        }
    }

    fn accept_object(&mut self, mut object: RefMut<Object>) {
        match self {
            Self::Delete(ids) => { object.remove_tuples(ids) },
            _ => {},
        }
    }

    fn into_results(self) -> QueryResults {
        match self {
            Self::Count(count) => QueryResults::count(count as u32),
            Self::Return(attributes, results) => {
                QueryResults{
                    attributes: attributes.into_iter().map(|attr| attr.name.to_string()).collect(),
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
    attrs.iter().map(|attr| Cell::from_bytes(attr.kind, tuple.cell_by_attr(attr).bytes())).collect()
}

pub struct RawObjectView<'a> {
    object: Ref<'a, Object>,
}

impl<'a> RawObjectView<'a> {
    pub fn raw_tuples(&self) -> &[ByteTuple] {
        &self.object.tuples
    }
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

        let insert_query = Query::tuple(&["1".to_string(), "something".to_string()]).insert_into("document");
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

        let result = db.execute_query(&Query::tuple(&["not-a-number".to_string(), "random-text".to_string()]).insert_into("document"));
        assert!(result.is_err());
    }

    #[test]
    pub fn filter() {
        let mut db = Database::default();
        db.execute_create(&Command::create_table("document").column("id", Type::NUMBER).column("content", Type::TEXT));

        for i in 1..20 {
            let content = format!("example{}", i);
            db.execute_query(&Query::tuple(&[i.to_string(), content]).insert_into("document")).expect("Insert");
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
    fn count() {
        let mut db = Database::default();
        db.execute_create(&Command::create_table("document").column("id", Type::NUMBER).column("content", Type::TEXT));

        for i in 1..21 {
            db.execute_query(&Query::tuple(&[i.to_string(), "example".to_string()]).insert_into("document")).expect("Insert");
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
        db.create_table("document").indexed_column("id", Type::NUMBER).column("content", Type::TEXT).create();
        let table = db.schema.find_relation("document").unwrap();

        db.execute_plan(Plan::insert(table, &["1".to_string(), "orig content".to_string()])).unwrap();
        db.execute_plan(Plan::insert(table, &["1".to_string(), "new content".to_string()])).unwrap();

        let result = db.execute_plan(Plan::scan(table)).unwrap();
        println!("{:?}", result.results().get(0));
        assert_eq!(result.results().get(0).unwrap().cell_by_name("document.content").unwrap().as_string(), "new content");
        assert_eq!(result.size(), 1);
    }

    #[test]
    fn recover_object() {
        let mut db = Database::default();
        db.create_table("document").column("id", Type::NUMBER).column("content", Type::TEXT).create();
        let table = db.schema.find_relation("document").unwrap();

        db.execute_plan(Plan::insert(table, &["1".to_string(), "one".to_string()])).unwrap();
        let snapshot = db.raw_object("document").unwrap().raw_tuples().to_vec();

        db.execute_plan(Plan::insert(table, &["2".to_string(), "two".to_string()])).unwrap();
        let all = db.execute_plan(Plan::scan(table)).unwrap();
        assert_eq!(all.size(), 2);

        db.recover_object(0, snapshot);
        let all = db.execute_plan(Plan::scan(table)).unwrap();
        assert_eq!(all.size(), 1);
    }
}
