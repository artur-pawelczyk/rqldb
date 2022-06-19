use std::collections::HashMap;
use std::iter::zip;
use std::cell::RefCell;

use crate::dsl::{Command, Query, Source, Finisher};
use crate::schema::{Column, Schema, Type, Relation};
use crate::{Cell, QueryResults};
use crate::plan;

pub trait TupleTrait {
    fn cell_at(&self, pos: u32) -> Option<&Cell>;
}

#[derive(Default)]
pub struct Database {
    schema: Schema,
    objects: HashMap<String, RefCell<Object>>
}

type Object = Vec<ByteTuple>;
type ByteTuple = Vec<Vec<u8>>;

#[derive(Debug)]
struct TupleSet {
    attributes: Vec<Attribute>,
    raw_tuples: Vec<Tuple>,
}

#[derive(Clone, Debug)]
pub enum Attribute {
    Unnamed(Type, i32),
    Named(Type, String),
    Absolute(Type, String, String),
}

#[derive(Clone, Debug)]
struct Tuple {
    contents: Vec<Cell>,
}

impl Tuple {
    fn from_bytes(types: &[Type], bytes: &[Vec<u8>]) -> Tuple {
        let cells: Vec<Cell> = zip(types, bytes).map(|(kind, b)| Cell::from_bytes(*kind, b)).collect();
        Self{
            contents: cells
        }
    }

    fn from_cells(cells: Vec<Cell>) -> Self {
        Self{ contents: cells }
    }


    pub fn into_cells(self) -> Vec<Cell> {
        self.contents
    }

    pub fn contents(&self) -> &[Cell] {
        &self.contents
    }

    fn add_cells(mut self, other: &impl TupleTrait) -> Self {
        let mut i = 0;
        while let Some(cell) = other.cell_at(i) {
            i += 1;
            self.contents.push(cell.clone());
        }

        self
    }    
}


impl TupleTrait for Tuple {
    fn cell_at(&self, i: u32) -> Option<&Cell> {
        self.contents.get(i as usize)
    }
}

impl Database {
    pub fn execute_create(&mut self, command: &Command) {
        self.schema.add_relation(&command.name, &command.columns);
        self.objects.insert(command.name.clone(), RefCell::new(Object::new()));
    }

    pub fn execute_query(&self, query: &Query) -> Result<QueryResults, &str> {
        let plan = plan::compute_plan(&self.schema, query)?;
        let source_tuples = read_source(self, &query.source)?;
        let joined_tuples = execute_join(self, source_tuples, &plan.joins)?;
        let filtered_tuples = filter_tuples(joined_tuples, &plan.filters)?;
        let final_attributes: Vec<Attribute> = plan.final_attributes.iter().map(|name| Attribute::from_full_name(name)).collect();

        match query.finisher {
            Finisher::AllColumns => Result::Ok(filtered_tuples.into_query_results(final_attributes)),
            Finisher::Columns(_) => todo!(),
            Finisher::Insert(_) => Result::Err("Can't run a mutating query"),
            Finisher::Count => Ok(QueryResults::count(filtered_tuples.count())),
        }
    }

    pub fn execute_mut_query(&mut self, query: &Query) -> Result<QueryResults, &str> {
        let plan = plan::compute_plan(&self.schema, query)?;
        let source_tuples = read_source(self, &query.source)?;
        let joined_tuples = execute_join(self, source_tuples, &plan.joins)?;
        let filtered_tuples = filter_tuples(joined_tuples, &plan.filters)?;

        match &query.finisher {
            Finisher::Insert(table) => {
                let table_schema = self.schema.find_relation(table).ok_or("No such table")?;
                let mut object = self.objects.get(table).expect("This shouldn't happen").borrow_mut();
                for tuple in filtered_tuples.iter() {
                    if !validate_with_schema(&table_schema.columns, tuple) { return Err("Invalid input") }
                    object.push(tuple.contents().iter().map(|x| x.as_bytes()).collect())
                }

                Result::Ok(filtered_tuples.into_query_results(vec![]))
            },
            Finisher::AllColumns => Result::Ok(filtered_tuples.into_query_results(vec![])),
            Finisher::Columns(_) => todo!(),
            Finisher::Count => Ok(QueryResults::count(filtered_tuples.count())),
        }
    }

    fn scan_table(&self, name: &str) -> Result<TupleSet, &'static str> {
        let rel = self.schema.find_relation(name).ok_or("No such relation in schema")?;
        let tuple_set = TupleSet::from_object(rel, &self.objects.get(name).ok_or("Could not find the object")?.borrow());

        Ok(tuple_set)
    }
}

fn validate_with_schema(columns: &[Column], tuple: &Tuple) -> bool {
    if columns.len() == tuple.contents().len() {
        zip(columns, tuple.contents()).all(|(col, cell)| col.kind == cell.kind)
    } else {
        false
    }
}

fn read_source(db: &Database, source: &Source) -> Result<TupleSet, &'static str> {
    return match &source {
        Source::TableScan(name) => {
            db.scan_table(name)
        },
        Source::Tuple(values) => {
            let cells = values.iter().map(|x| Cell::from_string(x)).collect();
            Ok(TupleSet::single_from_cells(cells))
        }
    }
}

fn execute_join(db: &Database, mut current_tuples: TupleSet, joins: &[plan::Join]) -> Result<TupleSet, &'static str> {
    match joins {
        [] => Ok(current_tuples),
        [join] => {
            let joiner = db.scan_table(join.source_table())?;

            for attr in &joiner.attributes {
                current_tuples.attributes.push(attr.clone());
            }

            let joined = current_tuples.map_mut(|joinee| {
                join.find_match(&joiner.raw_tuples, &joinee).map(|t| joinee.add_cells(t))
            });

            Ok(joined)
        }
        _ => todo!(),
    }
}

fn filter_tuples(mut source: TupleSet, filters: &[plan::Filter]) -> Result<TupleSet, &'static str> {
    match filters {
        [] => Ok(source),
        filters => {
            for filter in filters {
                source = apply_filter(source, filter);
            }

            Ok(source)
        },
    }
}

fn apply_filter(source: TupleSet, filter: &plan::Filter) -> TupleSet {
    source.filter(|tuple| filter.matches_tuple(tuple))
}

pub trait TupleSearch {
    fn search<F>(&self, fun: F) -> Option<&dyn TupleTrait>
    where F: Fn(&dyn TupleTrait) -> bool;
}

impl TupleSet {
    fn single_from_cells(cells: Vec<Cell>) -> Self {
        let attributes: Vec<Attribute> = cells.iter().enumerate().map(|(i, c)| Attribute::Unnamed(c.kind, i as i32)).collect();
        Self{ attributes, raw_tuples: vec![Tuple::from_cells(cells)] }
    }

    fn from_object(rel: &Relation, object: &Object) -> Self {
        let types = rel.types();
        let raw_tuples = object.iter().map(|val| Tuple::from_bytes(&types, val)).collect();
        Self{ raw_tuples, attributes: vec![] }
    }

    fn filter<F>(mut self, predicate: F) -> Self
    where F: Fn(&Tuple) -> bool {
        let mut filtered = vec![];

        for raw_tuple in self.raw_tuples {
            if predicate(&raw_tuple) {
                filtered.push(raw_tuple);
            }
        }

        self.raw_tuples = filtered;
        self
    }

    fn map_mut<F>(mut self, func: F) -> Self
    where F: Fn(Tuple) -> Option<Tuple> {
        let mut mapped = vec![];

        for raw_tuple in self.raw_tuples {
            func(raw_tuple).into_iter().for_each(|t| mapped.push(t));
        }
        
        self.raw_tuples = mapped;
        self
    }

    fn iter(&self) -> std::slice::Iter<Tuple> {
        self.raw_tuples.iter()
    }

    fn count(&self) -> i32 {
        self.raw_tuples.len() as i32
    }

    fn into_query_results(self, attributes: Vec<Attribute>) -> QueryResults {
        QueryResults{
            attributes: attributes.into_iter().map(|x| x.as_string()).collect(),
            results: self.raw_tuples.into_iter().map(Tuple::into_cells).collect()
        }
    }
}

impl Attribute {
    fn from_full_name(s: &str) -> Self {
        let parts: Vec<_> = s.split('.').collect();
        match parts[..] {
            [a] => Self::Named(Type::default(), a.to_string()),
            [a, b] => Self::Absolute(Type::default(), a.to_string(), b.to_string()),
            _ => panic!(),
        }
    }

    fn as_string(&self) -> String {
        match self {
            Attribute::Unnamed(_, i) => i.to_string(),
            Attribute::Named(_, s) => s.to_string(),
            Attribute::Absolute(_, t, s) => format!("{}.{}", t, s),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dsl::Operator;

    #[test]
    fn query_not_existing_relation() {
        let query = Query::scan("not_real_relation").select_all();
        let db = Database::default();
        let result = db.execute_query(&query);
        assert!(!result.is_ok());
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
        let insert_result = db.execute_mut_query(&insert_query);
        assert!(insert_result.is_ok());

        let query = Query::scan("document").select_all();
        let result = db.execute_query(&query);
        assert!(result.is_ok());
        let tuples = result.unwrap();
        assert_eq!(tuples.size(), 1);
        let results = tuples.results();
        let tuple = results.get(0).expect("fail");
        assert_eq!(&tuple.contents[0].as_bytes(), &Vec::from(1_i32.to_be_bytes()));
        assert_eq!(&tuple.contents[1].as_string(), "something");
    }

    #[test]
    pub fn failed_insert() {
        let mut db = Database::default();

        let command = Command::create_table("document")
            .column("id", Type::NUMBER)
            .column("content", Type::TEXT);
        db.execute_create(&command);

        let result = db.execute_mut_query(&Query::tuple(&["not-a-number".to_string(), "random-text".to_string()]).insert_into("document"));
        assert!(result.is_err());
    }

    #[test]
    pub fn filter() {
        let mut db = Database::default();
        db.execute_create(&Command::create_table("document").column("id", Type::NUMBER).column("content", Type::TEXT));

        for i in 1..20 {
            let content = format!("example{}", i);
            db.execute_mut_query(&Query::tuple(&[i.to_string(), content]).insert_into("document")).expect("Insert");
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

        db.execute_mut_query(&Query::tuple(&["1", "example", "2"]).insert_into("document")).unwrap();
        db.execute_mut_query(&Query::tuple(&["1", "type_a"]).insert_into("type")).unwrap();
        db.execute_mut_query(&Query::tuple(&["2", "type_b"]).insert_into("type")).unwrap();

        let result = db.execute_query(&Query::scan("document").join("type", "document.type_id", "type.id")).unwrap();
        assert_eq!(*result.attributes, ["document.id", "document.content", "document.type_id", "type.id", "type.name"]);
        let tuple = result.tuple_at(0).unwrap();
        let document_id = tuple.cell_by_name("document.id").unwrap().as_string();
        let type_name = tuple.cell_by_name("type.name").unwrap().as_string();
        assert_eq!(document_id, "1");
        assert_eq!(type_name, "type_b");
    }

    #[test]
    pub fn count() {
        let mut db = Database::default();
        db.execute_create(&Command::create_table("document").column("id", Type::NUMBER).column("content", Type::TEXT));

        for i in 1..21 {
            db.execute_mut_query(&Query::tuple(&[i.to_string(), "example".to_string()]).insert_into("document")).expect("Insert");
        }

        let result = db.execute_query(&Query::scan("document").count()).unwrap();
        let count = result.results().get(0)
            .and_then(|t| t.cell_at(0))
            .and_then(|c| c.as_number())
            .unwrap();
        assert_eq!(count, 20);
    }
}
