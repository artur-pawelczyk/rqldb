use std::collections::HashMap;
use std::rc::Rc;
use std::ops::Deref;
use std::fmt;
use std::iter::zip;

use crate::select::{SelectQuery, Source, Finisher};
use crate::schema::{Column, Schema, Type};
use crate::create::CreateRelationCommand;

pub struct Database {
    schema: Schema,
    objects: HashMap<String, Object>
}

type Object = Vec<ByteTuple>;
type ByteTuple = Vec<Vec<u8>>;

pub struct QueryResults {
    pub results: Rc<Vec<Tuple>>,
    pub attributes: Rc<Vec<String>>
}

pub struct Tuple {
    pub contents: Vec<Cell>
}

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

    pub fn into_string(&self) -> String {
        match self.kind {
            Type::NUMBER => {
                let bytes: [u8; 4] = self.contents.clone().try_into().unwrap();
                i32::from_be_bytes(bytes).to_string()
            },
            Type::TEXT => String::from_utf8(self.contents.clone()).unwrap(),
            _ => todo!(),
        }
    }

    fn as_bytes(&self) -> Vec<u8> {
        self.contents.clone()
    }
}

impl fmt::Debug for Cell {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.into_string())
    }
}


impl Default for Database {
    fn default() -> Self {
        Self{schema: Schema::default(), objects: HashMap::new()}
    }
}

impl Database {
    pub fn execute_create(&mut self, command: &CreateRelationCommand) {
        self.schema.add_relation(&command.name, &command.columns);
        self.objects.insert(command.name.clone(), Object::new());
    }

    pub fn execute_query(&self, query: &SelectQuery) -> Result<QueryResults, &str> {
        let source_tuples = read_source(self, &query.source)?;

        match query.finisher {
            Finisher::AllColumns => Result::Ok(source_tuples),
            Finisher::Columns(_) => todo!(),
            Finisher::Insert(_) => Result::Err("Can't run a mutating query")
        }
    }

    pub fn execute_mut_query(&mut self, query: &SelectQuery) -> Result<QueryResults, &str> {
        let source_tuples = read_source(self, &query.source)?;

        match &query.finisher {
            Finisher::Insert(table) => {
                let table_schema = self.schema.find_relation(table).unwrap();
                let object = self.objects.entry(table.to_string()).or_insert(Object::new());
                for tuple in source_tuples.results().deref() {
                    if !validate_with_schema(&table_schema.columns, tuple) { return Err("Invalid input") }
                    object.push(tuple.contents.iter().map(|x| x.as_bytes()).collect())
                }

                Result::Ok(source_tuples)
            },
            Finisher::AllColumns => Result::Ok(source_tuples),
            Finisher::Columns(_) => todo!(),
        }
    }

}

fn validate_with_schema(columns: &[Column], tuple: &Tuple) -> bool {
    if columns.len() == tuple.len() {
        zip(columns, &tuple.contents).all(|(col, cell)| col.kind == cell.kind)
    } else {
        false
    }
}

fn read_source(db: &Database, source: &Source) -> Result<QueryResults, &'static str> {
    return match &source {
        Source::TableScan(name) => {
            let rel = db.schema.find_relation(name).ok_or("No such relation in schema")?;
            let attributes = rel.columns.iter().map(|col| col.name.clone()).collect();
            let types: Vec<Type> = rel.columns.iter().map(|col| col.kind).collect();
            let values = db.objects.get(name).ok_or("Could not find the object")?;
            let tuples = values.iter().map(|x| Tuple::from_bytes(&types, x)).collect();
            Ok(QueryResults{attributes: Rc::new(attributes), results: Rc::new(tuples)})
        },
        Source::Tuple(values) => {
            let cells = values.iter().map(|x| Cell::from_string(x)).collect();
            let tuple = Tuple{contents: cells};
            Ok(QueryResults{attributes: Rc::new(vec![]), results: Rc::new(vec![tuple])})
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
        let mut db = Database::new();
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
        let mut db = Database::new();

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
        assert_eq!(&tuple.contents[0].into_bytes(), &Vec::from(1_i32.to_be_bytes()));
        assert_eq!(&tuple.contents[1].into_string(), "something");
    }

    #[test]
    pub fn failed_insert() {
        let mut db = Database::new();

        let command = CreateRelationCommand::with_name("document")
            .column("id", Type::NUMBER)
            .column("content", Type::TEXT);
        db.execute_create(&command);

        let result = db.execute_mut_query(&SelectQuery::tuple(&["not-a-number".to_string(), "random-text".to_string()]).insert_into("document"));
        assert!(result.is_err());
    }
}
