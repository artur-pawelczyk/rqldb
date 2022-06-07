use std::collections::HashMap;
use std::rc::Rc;
use std::fmt;
use std::iter::zip;

use crate::select::{SelectQuery, Source, Finisher, Operator, Filter, JoinSource};
use crate::schema::{Column, Schema, Type};
use crate::create::CreateRelationCommand;

#[derive(Default)]
pub struct Database {
    schema: Schema,
    objects: HashMap<String, Object>
}

type Object = Vec<ByteTuple>;
type ByteTuple = Vec<Vec<u8>>;

pub struct QueryResults {
    results: Rc<Vec<Tuple>>,
    attributes: Rc<Vec<String>>
}

pub enum TupleSet {
    Named(String, Vec<Attribute>, Vec<Tuple>),
    Unnamed(Vec<Attribute>, Vec<Tuple>),
}

pub enum Attribute {
    Unnamed(i32),
    Named(String),
    Absolute(String, String),
}

#[derive(Clone, Debug)]
pub struct Tuple {
    pub contents: Vec<Cell>
}

#[derive(Clone)]
pub struct Cell {
    contents: Vec<u8>,
    kind: Type
}

impl Tuple {
    fn from_bytes(types: &[Type], bytes: &Vec<Vec<u8>>) -> Tuple {
        let cells: Vec<Cell> = zip(types, bytes).map(|(t, b)| Cell::from_bytes(*t, b)).collect();
        Tuple{contents: cells}
    }

    fn len(&self) -> usize {
        self.contents.len()
    }

    fn cell_at(&self, i: u32) -> Option<&Cell> {
        self.contents.get(i as usize)
    }

    fn join(mut self, other: &Tuple) -> Self {
        for t in &other.contents {
            self.contents.push(t.clone());
        }
        self
    }
}

impl Cell {
    fn from_bytes(kind: Type, bytes: &[u8]) -> Cell {
        if let Some(first) = bytes.get(0) {
            let firstchar = *first as char;
            if let Some(num) = firstchar.to_digit(8) {
                return Cell{contents: vec![num as u8], kind}
            }
        }

        Cell{contents: Vec::from(bytes), kind}
    }

    fn from_string(source: &str) -> Cell {
        if let Result::Ok(number) = source.parse::<i32>() {
            Cell{contents: Vec::from(number.to_be_bytes()), kind: Type::NUMBER}
        } else {
            Cell{contents: Vec::from(source.as_bytes()), kind: Type::TEXT}
        }
    }

    pub fn as_string(&self) -> String {
        match self.kind {
            Type::NUMBER => {
                self.as_number().unwrap().to_string()
            },
            Type::TEXT => String::from_utf8(self.contents.clone()).unwrap(),
            _ => todo!(),
        }
    }

    fn as_bytes(&self) -> Vec<u8> {
        self.contents.clone()
    }

    pub fn as_number(&self) -> Option<i32> {
        match self.kind {
            Type::NUMBER => {
                let bytes: Result<[u8; 4], _> = self.contents.clone().try_into();
                match bytes {
                    Ok(x) => Some(i32::from_be_bytes(x)),
                    _ => None
                }
            },
            _ => None
        }
    }
}

impl fmt::Debug for Cell {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.as_string())
    }
}


impl Database {
    pub fn execute_create(&mut self, command: &CreateRelationCommand) {
        self.schema.add_relation(&command.name, &command.columns);
        self.objects.insert(command.name.clone(), Object::new());
    }

    pub fn execute_query(&self, query: &SelectQuery) -> Result<QueryResults, &str> {
        let source_tuples = read_source(self, &query.source)?;
        let joined_tuples = execute_join(self, source_tuples, &query.join_sources)?;
        let filtered_tuples = filter_tuples(joined_tuples, &query.filters);

        match query.finisher {
            Finisher::AllColumns => Result::Ok(QueryResults::from(filtered_tuples)),
            Finisher::Columns(_) => todo!(),
            Finisher::Insert(_) => Result::Err("Can't run a mutating query")
        }
    }

    pub fn execute_mut_query(&mut self, query: &SelectQuery) -> Result<QueryResults, &str> {
        let source_tuples = read_source(self, &query.source)?;

        match &query.finisher {
            Finisher::Insert(table) => {
                let table_schema = self.schema.find_relation(table).ok_or("No such table")?;
                let object = self.objects.entry(table.to_string()).or_insert(Object::new());
                for tuple in source_tuples.contents() {
                    if !validate_with_schema(&table_schema.columns, tuple) { return Err("Invalid input") }
                    object.push(tuple.contents.iter().map(|x| x.as_bytes()).collect())
                }

                Result::Ok(QueryResults::from(source_tuples))
            },
            Finisher::AllColumns => Result::Ok(QueryResults::from(source_tuples)),
            Finisher::Columns(_) => todo!(),
        }
    }

    fn scan_table(&self, name: &str) -> Result<TupleSet, &'static str> {
        let rel = self.schema.find_relation(name).ok_or("No such relation in schema")?;
        let attributes = rel.columns.iter().map(|col| col.name.clone()).map(|x| Attribute::Absolute(name.to_string(), x)).collect();
        let types: Vec<Type> = rel.columns.iter().map(|col| col.kind).collect();
        let values = self.objects.get(name).ok_or("Could not find the object")?;
        let tuples = values.iter().map(|x| Tuple::from_bytes(&types, x)).collect();
        Ok(TupleSet::Named(name.to_string(), attributes, tuples))    }
}

fn validate_with_schema(columns: &[Column], tuple: &Tuple) -> bool {
    if columns.len() == tuple.len() {
        zip(columns, &tuple.contents).all(|(col, cell)| col.kind == cell.kind)
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
            let tuple = Tuple{contents: cells};
            Ok(TupleSet::Unnamed(vec![], vec![tuple]))
        }
    }
}

fn execute_join(db: &Database, mut current_tuples: TupleSet, joins: &[JoinSource]) -> Result<TupleSet, &'static str> {
    match joins {
        [] => Ok(current_tuples),
        [join] => {
            let our = current_tuples.take_contents();
            let mut theirs = db.scan_table(&join.table)?;
            let mut joined: Vec<Tuple> = Vec::with_capacity(our.len());
            for our_tuple in our {
                if let Some(their_tuple) = match_tuple_for_join(&our_tuple, &theirs) {
                    joined.push(our_tuple.join(their_tuple));
                }
            }

            let mut our_attrs = current_tuples.take_attributes();
            let their_attrs = theirs.take_attributes();
            for attr in their_attrs {
                our_attrs.push(attr);
            }
            
            Ok(TupleSet::Unnamed(our_attrs, joined))
        }
        _ => todo!(),
    }
}

fn match_tuple_for_join<'a>(tuple: &Tuple, joined_table: &'a TupleSet) -> Option<&'a Tuple> {
    let f_id = tuple.cell_at(1).map(|c| c.as_number())?;
    joined_table.contents().iter().find(|t| t.cell_at(0).map(|c| c.as_number()) == Some(f_id))
}

fn filter_tuples(source: TupleSet, filters: &[Filter]) -> TupleSet {
    match filters {
        [] => source,
        filters => {
            match source {
                TupleSet::Named(name, attributes, mut tuples) => {
                    for filter in filters {
                        tuples = apply_filter(tuples, filter);
                    }

                    TupleSet::Named(name, attributes, tuples)
                },
                TupleSet::Unnamed(attributes, mut tuples) => {
                    for filter in filters {
                        tuples = apply_filter(tuples, filter);
                    }

                    TupleSet::Unnamed(attributes, tuples)
                }
            }
        },
    }
}

fn apply_filter(source: Vec<Tuple>, filter: &Filter) -> Vec<Tuple> {
    source.into_iter()
        .filter(|tuple| test_filter(filter, tuple))
        .collect()
}

fn test_filter(filter: &Filter, tuple: &Tuple) -> bool {
    match filter {
        Filter::Condition(left, op, right) => {
            assert_eq!(left, "id");
            let cell = tuple.cell_at(0).unwrap();
            let left_n = cell.as_number().unwrap();
            let right_n: i32 = right.parse().unwrap();
            match op {
                Operator::EQ => left_n == right_n,
                Operator::GT => left_n > right_n,
                Operator::GE => left_n >= right_n,
                Operator::LT => left_n < right_n,
                Operator::LE => left_n <= right_n,
            }
        }
    }
}

impl QueryResults {
    pub fn empty(attributes: Vec<String>) -> Self {
        Self{attributes: Rc::new(attributes), results: Rc::new(vec![])}
    }

    pub fn single_unnamed(values: Tuple) -> Self {
        Self{attributes: Rc::new(vec![]), results: Rc::new(vec![values])}
    }

    pub fn from(tuple_set: TupleSet) -> Self {
        match tuple_set {
            TupleSet::Named(_, attributes, contents) => Self{
                attributes: Rc::new(simplify_attributes(attributes).iter().map(|x| x.as_string().to_string()).collect()),
                results: Rc::new(contents)
            },
            TupleSet::Unnamed(attributes, contents) => Self{
                attributes: Rc::new(simplify_attributes(attributes).iter().map(|x| x.as_string().to_string()).collect()),
                results: Rc::new(contents)
            },
        }
    }

    pub fn size(&self) -> u32 {
        self.results.len() as u32
    }

    pub fn attributes(&self) -> Rc<Vec<String>> {
        Rc::clone(&self.attributes)
    }

    pub fn results(&self) -> Rc<Vec<Tuple>> {
        Rc::clone(&self.results)
    }
}

/// If all the attributes are in the same table, make them all non-absolute
fn simplify_attributes(attrs: Vec<Attribute>) -> Vec<Attribute> {
    let mut tables: Vec<&str> = attrs.iter().flat_map(|attr| attr.table()).collect();
    tables.dedup();

    if tables.len() == 1 {
        attrs.into_iter().map(|attr| attr.into_named()).collect()
    } else {
        attrs
    }
}

impl TupleSet {
    fn take_attributes(&mut self) -> Vec<Attribute> {
        match self {
            TupleSet::Named(_, attrs, _) => std::mem::take(attrs),
            _ => todo!(),
        }
    }

    fn _attributes(&self) -> &[Attribute] {
        match self {
            TupleSet::Named(_, attrs, _) => attrs,
            TupleSet::Unnamed(attrs, _) => attrs,
        }
    }

    fn contents(&self) -> &[Tuple] {
        match self {
            TupleSet::Named(_, _, x) => x,
            TupleSet::Unnamed(_, x) => x,
        }
    }

    fn take_contents(&mut self) -> Vec<Tuple> {
        match self {
            TupleSet::Named(_, _, x) => std::mem::take(x),
            TupleSet::Unnamed(_, x) => std::mem::take(x),
        }
    }
}

impl Attribute {
    fn as_string(&self) -> String {
        match self {
            Attribute::Unnamed(i) => i.to_string(),
            Attribute::Named(s) => s.to_string(),
            Attribute::Absolute(t, s) => format!("{}.{}", t, s),
        }
    }

    fn table<'a>(&'a self) -> Option<&'a str> {
        match self {
            Attribute::Absolute(t, _) => Some(t),
            _ => None,
        }
    }

    fn into_named(self) -> Attribute {
        match self {
            Attribute::Absolute(_, n) => Attribute::Named(n),
            Attribute::Named(n) => Attribute::Named(n),
            Attribute::Unnamed(i) => Attribute::Named(i.to_string()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn query_not_existing_relation() {
        let query = SelectQuery::scan("not_real_relation").select_all();
        let db = Database::default();
        let result = db.execute_query(&query);
        assert!(!result.is_ok());
    }

    #[test]
    fn query_empty_relation() {
        let mut db = Database::default();
        let command = CreateRelationCommand::with_name("document")
            .column("id", Type::NUMBER)
            .column("content", Type::TEXT);

        db.execute_create(&command);

        let query = SelectQuery::scan("document").select_all();
        let result = db.execute_query(&query);

        assert!(result.is_ok());
        let tuples = result.unwrap();
        assert_eq!(tuples.size(), 0);
        let attrs = tuples.attributes();
        assert_eq!(attrs.as_slice(), ["id".to_string(), "content".to_string()]);
    }

    #[test]
    pub fn insert() {
        let mut db = Database::default();

        let command = CreateRelationCommand::with_name("document")
            .column("id", Type::NUMBER)
            .column("content", Type::TEXT);
        db.execute_create(&command);

        let insert_query = SelectQuery::tuple(&["1".to_string(), "something".to_string()]).insert_into("document");
        let insert_result = db.execute_mut_query(&insert_query);
        assert!(insert_result.is_ok());

        let query = SelectQuery::scan("document").select_all();
        let result = db.execute_query(&query);
        assert!(result.is_ok());
        let tuples = result.unwrap();
        assert_eq!(tuples.size(), 1);
        let results = tuples.results();
        let tuple = results.iter().next().expect("fail");
        assert_eq!(&tuple.contents[0].as_bytes(), &Vec::from(1_i32.to_be_bytes()));
        assert_eq!(&tuple.contents[1].as_string(), "something");
    }

    #[test]
    pub fn failed_insert() {
        let mut db = Database::default();

        let command = CreateRelationCommand::with_name("document")
            .column("id", Type::NUMBER)
            .column("content", Type::TEXT);
        db.execute_create(&command);

        let result = db.execute_mut_query(&SelectQuery::tuple(&["not-a-number".to_string(), "random-text".to_string()]).insert_into("document"));
        assert!(result.is_err());
    }

    #[test]
    pub fn filter() {
        let mut db = Database::default();
        db.execute_create(&CreateRelationCommand::with_name("document").column("id", Type::NUMBER).column("content", Type::TEXT));

        for i in 1..20 {
            db.execute_mut_query(&SelectQuery::tuple(&[i.to_string(), "example".to_string()]).insert_into("document")).expect("Insert");
        }

        let mut result = db.execute_query(&SelectQuery::scan("document").filter("id", Operator::EQ, "5")).unwrap();
        assert_eq!(result.size(), 1);

        result = db.execute_query(&SelectQuery::scan("document").filter("id", Operator::GT, "5").filter("id", Operator::LT, "10")).unwrap();
        assert_eq!(result.size(), 4);
    }

    #[test]
    pub fn join() {
        let mut db = Database::default();
        db.execute_create(&CreateRelationCommand::with_name("document").column("id", Type::NUMBER).column("type_id", Type::NUMBER));
        db.execute_create(&CreateRelationCommand::with_name("type").column("id", Type::NUMBER).column("name", Type::TEXT));

        db.execute_mut_query(&SelectQuery::tuple(&["1", "2"]).insert_into("document")).unwrap();
        db.execute_mut_query(&SelectQuery::tuple(&["1", "type_a"]).insert_into("type")).unwrap();
        db.execute_mut_query(&SelectQuery::tuple(&["2", "type_b"]).insert_into("type")).unwrap();

        let result = db.execute_query(&SelectQuery::scan("document").join("type", "document.type_id", "type.id")).unwrap();
        assert_eq!(*result.attributes, ["document.id", "document.type_id", "type.id", "type.name"]);
        let tuple = result.results.get(0).unwrap();
        let document_id = tuple.cell_at(0).unwrap().as_string();
        let type_name = tuple.cell_at(3).unwrap().as_string();
        assert_eq!(document_id, "1");
        assert_eq!(type_name, "type_b");
    }
}
