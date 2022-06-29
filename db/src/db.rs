use std::collections::HashMap;
use std::iter::zip;
use std::cell::RefCell;

use crate::dsl::{Command, Query, Finisher};
use crate::schema::{Schema, Type, Relation};
use crate::{Cell, QueryResults};
use crate::plan;

pub(crate) trait Tuple {
    fn cell(&self, attr: &plan::Attribute) -> Cell;
    fn all_cells(&self, attrs: &[plan::Attribute]) -> Vec<Cell> {
        attrs.iter().map(|a| self.cell(a)).collect()
    }
}

#[derive(Default)]
pub struct Database {
    schema: Schema,
    objects: HashMap<String, RefCell<Object>>
}

type Object = Vec<ByteTuple>;
type ByteTuple = Vec<Vec<u8>>;

#[derive(Debug)]
struct EagerTupleSet<T: Tuple> {
    raw_tuples: Vec<T>,
}

#[derive(Clone, Debug)]
struct EagerTuple {
    contents: Vec<Cell>,
}

impl EagerTuple {
    fn from_bytes(types: &[Type], bytes: &[Vec<u8>]) -> EagerTuple {
        let cells: Vec<Cell> = zip(types, bytes).map(|(kind, b)| Cell::from_bytes(*kind, b)).collect();
        EagerTuple{
            contents: cells
        }
    }

    fn from_cells(cells: Vec<Cell>) -> Self {
        EagerTuple{ contents: cells }
    }

    pub fn contents(&self) -> &[Cell] {
        &self.contents
    }

    fn add_cells(mut self, other: &impl Tuple, other_attrs: &[plan::Attribute]) -> Self {
        self.contents.extend(other.all_cells(other_attrs));
        self
    }
}


impl Tuple for EagerTuple {
    fn cell(&self, attr: &plan::Attribute) -> Cell {
        self.contents.get(attr.pos).expect("Already validated").clone()
    }

    fn all_cells(&self, _: &[plan::Attribute]) -> Vec<Cell> {
        self.contents.to_vec()
    }
}

struct ObjectTuple<'a> {
    raw: &'a ByteTuple,
}

impl<'a> Tuple for ObjectTuple<'a> {
    fn cell(&self, attr: &plan::Attribute) -> Cell {
        Cell::from_bytes(attr.kind, self.raw.get(attr.pos).unwrap())
    }
}

impl Database {
    pub fn execute_create(&mut self, command: &Command) {
        self.schema.add_relation(&command.name, &command.columns);
        self.objects.insert(command.name.clone(), RefCell::new(Object::new()));
    }

    pub fn execute_query(&self, query: &Query) -> Result<QueryResults, &str> {
        let plan = plan::compute_plan(&self.schema, query)?;
        let source_tuples = read_source(self, &plan.source)?;
        let joined_tuples = execute_join(self, source_tuples, &plan)?;
        let filtered_tuples = filter_tuples(joined_tuples, &plan.filters)?;

        match &query.finisher {
            Finisher::AllColumns => Result::Ok(filtered_tuples.into_query_results(&plan.final_attributes())),
            Finisher::Columns(_) => todo!(),
            Finisher::Count => Ok(QueryResults::count(filtered_tuples.count())),
            Finisher::Insert(table) => {
                let mut object = self.objects.get(table).expect("This shouldn't happen").borrow_mut();
                for tuple in filtered_tuples.iter() {
                    object.push(tuple.contents().iter().map(|x| x.as_bytes()).collect())
                }

                Result::Ok(filtered_tuples.into_query_results(&vec![]))
            }
        }
    }

    fn scan_table(&self, name: &str) -> Result<EagerTupleSet<EagerTuple>, &'static str> {
        let rel = self.schema.find_relation(name).ok_or("No such relation in schema")?;
        let tuple_set = EagerTupleSet::from_object(rel, &self.objects.get(name).ok_or("Could not find the object")?.borrow());

        Ok(tuple_set)
    }
}

fn read_source(db: &Database, source: &plan::Source) -> Result<EagerTupleSet<EagerTuple>, &'static str> {
    match &source.contents {
        plan::Contents::TableScan(rel) => db.scan_table(&rel.name),
        plan::Contents::Tuple(values) => Ok(EagerTupleSet::single_from_cells(values.iter().map(|x| Cell::from_string(x)).collect())),
        _ => panic!()
    }
}

fn execute_join(db: &Database, current_tuples: EagerTupleSet<EagerTuple>, plan: &plan::Plan) -> Result<EagerTupleSet<EagerTuple>, &'static str> {
    match &plan.joins[..] {
        [] => Ok(current_tuples.into_eager(&plan.source_attributes())),
        [join] => {
            let joiner = db.scan_table(join.source_table())?;
            let joined = current_tuples.into_eager(&join.joinee_attributes).map_mut(|joinee| {
                join.find_match(&joiner.raw_tuples, &joinee).map(|t| joinee.add_cells(t, &join.joiner_attributes))
            });

            Ok(joined)
        }
        _ => todo!(),
    }
}

fn filter_tuples(mut source: EagerTupleSet<EagerTuple>, filters: &[plan::Filter]) -> Result<EagerTupleSet<EagerTuple>, &'static str> {
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

fn apply_filter(source: EagerTupleSet<EagerTuple>, filter: &plan::Filter) -> EagerTupleSet<EagerTuple> {
    source.filter(|tuple| filter.matches_tuple(tuple))
}

pub(crate) trait TupleSearch {
    fn search<F>(&self, fun: F) -> Option<&dyn Tuple>
    where F: Fn(&dyn Tuple) -> bool;
}

trait TupleSet<T: Tuple> {
    fn filter<F>(self, predicate: F) -> Self
    where F: Fn(&T) -> bool;

    fn map_mut<F>(self, mapper: F) -> Self
    where F: Fn(T) -> Option<T>;

    fn count(&self) -> u32;

    fn iter<'a>(&'a self) -> Box<dyn Iterator<Item = &T> + 'a>;

    fn into_eager(self, attrs: &[plan::Attribute]) -> EagerTupleSet<EagerTuple>;

    fn into_query_results(self, attributes: &[plan::Attribute]) -> QueryResults;
}

impl EagerTupleSet<EagerTuple> {
    fn single_from_cells(cells: Vec<Cell>) -> Self {
        EagerTupleSet{ raw_tuples: vec![EagerTuple::from_cells(cells)] }
    }

    fn from_object(rel: &Relation, object: &Object) -> Self {
        let types = rel.types();
        let raw_tuples = object.iter().map(|val| EagerTuple::from_bytes(&types, val)).collect();
        EagerTupleSet{ raw_tuples }
    }


}

impl TupleSet<EagerTuple> for EagerTupleSet<EagerTuple> {
    fn filter<F>(mut self, predicate: F) -> Self
    where F: Fn(&EagerTuple) -> bool {
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
    where F: Fn(EagerTuple) -> Option<EagerTuple> {
        let mut mapped = vec![];

        for raw_tuple in self.raw_tuples {
            func(raw_tuple).into_iter().for_each(|t| mapped.push(t));
        }

        self.raw_tuples = mapped;
        self
    }

    fn iter<'a>(&'a self) -> Box<dyn Iterator<Item = &'a EagerTuple> + 'a> {
        Box::new(self.raw_tuples.iter())
    }

    fn count(&self) -> u32 {
        self.raw_tuples.len() as u32
    }

    fn into_eager(self, attrs: &[plan::Attribute]) -> EagerTupleSet<EagerTuple> {
        let new_tuples = self.raw_tuples.into_iter().map(|t| EagerTuple::from_cells(t.all_cells(attrs))).collect();
        EagerTupleSet{ raw_tuples: new_tuples }
    }

    fn into_query_results(self, attributes: &[plan::Attribute]) -> QueryResults {
        QueryResults{
            attributes: attributes.iter().map(|x| x.name.to_string()).collect(),
            results: self.raw_tuples.into_iter().map(|t| t.all_cells(attributes)).collect()
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
    pub fn count() {
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
}
