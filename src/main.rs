pub mod select;
pub mod create;
pub mod schema;

use std::collections::HashMap;
use std::rc::Rc;
use std::ops::Deref;
use std::fmt;

use select::{SelectQuery, Source, Finisher};
use schema::{Schema, Type};
use create::CreateRelationCommand;

struct Database {
    schema: Schema,
    objects: HashMap<String, Object>
}

type Object = Vec<ByteTuple>;
type ByteTuple = Vec<Vec<u8>>;

// TODO: Rename to something like "QueryResults"
pub struct Tuples {
    results: Rc<Vec<Tuple>>,
    attributes: Rc<Vec<String>>
}

pub struct Tuple {
    contents: Vec<Cell>
}

pub struct Cell {
    contents: Vec<u8>
}

impl Tuple {
    fn from_bytes(bytes: &Vec<Vec<u8>>) -> Tuple {
        let contents = bytes.iter().map(|cell_bytes| Cell::from_bytes(cell_bytes)).collect();
        Tuple{contents}
    }


}

impl Cell {
    fn from_bytes(bytes: &[u8]) -> Cell {
        if let Some(first) = bytes.get(0) {
            let firstchar = *first as char;
            if let Some(num) = firstchar.to_digit(8) {
                return Cell{contents: vec![num as u8]}
            }
        }

        Cell{contents: Vec::from(bytes)}
    }

    fn from_string(source: &str) -> Cell {
        if let Result::Ok(number) = source.parse::<u8>() {
            Cell{contents: vec![number]}
        } else {
            Cell{contents: Vec::from(source.as_bytes())}
        }
    }

    fn into_string(&self) -> String {
        if self.contents.len() == 1 {
            self.contents[0].to_string()
        } else {
            String::from_utf8(self.contents.clone()).unwrap()
        }
    }

    fn into_bytes(&self) -> Vec<u8> {
        self.contents.clone()
    }
}

impl fmt::Debug for Cell {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.into_string())
    }
}


impl Database {
    pub fn new() -> Self{
        Self{schema: Schema::new(), objects: HashMap::new()}
    }

    pub fn execute_create(&mut self, command: &CreateRelationCommand) {
        self.schema.add_relation(&command.name, &command.columns);
        self.objects.insert(command.name.clone(), Object::new());
    }

    pub fn execute_query(&self, query: &SelectQuery) -> Result<Tuples, &str> {
        let source_tuples = match &query.source {
            Source::TableScan(name) => {
                let rel = self.schema.find_relation(&name).ok_or("No such relation in schema")?;
                let attributes = rel.columns.iter().map(|col| col.name.clone()).collect();
                let values = self.objects.get(name).ok_or("Could not find the object")?;
                let tuples = values.iter().map(|x| Tuple::from_bytes(x)).collect();
                Tuples{attributes: Rc::new(attributes), results: Rc::new(tuples)}
            },
            Source::Tuple(values) => {
                let cells = values.iter().map(|x| Cell::from_string(x)).collect();
                let tuple = Tuple{contents: cells};
                Tuples{attributes: Rc::new(vec![]), results: Rc::new(vec![tuple])}
            }
        };

        match query.finisher {
            Finisher::AllColumns => Result::Ok(source_tuples),
            Finisher::Columns(_) => todo!(),
            Finisher::Insert(_) => Result::Err("Can't run a mutating query")
        }
    }

    pub fn execute_mut_query(&mut self, query: &SelectQuery) -> Result<Tuples, &str> {
        let source_tuples = match &query.source {
            Source::TableScan(name) => {
                let rel = self.schema.find_relation(&name).ok_or("No such relation in schema")?;
                let attributes = rel.columns.iter().map(|col| col.name.clone()).collect();
                let values = self.objects.get(name).ok_or("Could not find the object")?;
                let tuples = values.iter().map(|x| Tuple::from_bytes(x)).collect();
                Tuples{attributes: Rc::new(attributes), results: Rc::new(tuples)}
            },
            Source::Tuple(values) => {
                let cells = values.iter().map(|x| Cell::from_string(x)).collect();
                let tuple = Tuple{contents: cells};
                Tuples{attributes: Rc::new(vec![]), results: Rc::new(vec![tuple])}
            }
        };

        match &query.finisher {
            Finisher::Insert(table) => {
                let _tableSchema = self.schema.find_relation(table);
                let object = self.objects.entry(table.to_string()).or_insert(Object::new());
                for tuple in source_tuples.results().deref() {
                    object.push(tuple.contents.iter().map(|x| x.into_bytes()).collect())
                }

                Result::Ok(source_tuples)
            }
            _ => todo!()
        }
    }
}

impl Tuples {
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

fn main() {
    let mut db = Database::new();

    let command = CreateRelationCommand::with_name("document")
        .column("id", Type::NUMBER)
        .column("content", Type::TEXT);
    db.execute_create(&command);

    let insert_query = SelectQuery::tuple(&["1", "something"]).insert_into("document");
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
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn query_not_existing_relation() {
        let query = SelectQuery::scan("not_real_relation").select_all();
        let db = Database::new();
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

        let insert_query = SelectQuery::tuple(&["1", "something"]).insert_into("document");
        let insert_result = db.execute_mut_query(&insert_query);
        assert!(insert_result.is_ok());

        let query = SelectQuery::scan("document").select_all();
        let result = db.execute_query(&query);
        assert!(result.is_ok());
        let tuples = result.unwrap();
        assert_eq!(tuples.size(), 1);
        let results = tuples.results();
        let tuple = results.iter().next().expect("fail");
        assert_eq!(&tuple.contents[0].into_bytes(), &Vec::from(1_u8.to_be_bytes()));
        assert_eq!(&tuple.contents[1].into_string(), "something");
    }
}
