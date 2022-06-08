pub mod create;
pub mod db;
pub mod parse;
pub mod schema;
pub mod select;

use std::rc::Rc;
use std::fmt;

use crate::schema::Type;

pub struct QueryResults {
    attributes: Rc<Vec<String>>,
    results: Rc<Vec<Tuple>>,
}

pub struct Tuple {
    contents: Vec<Cell>,
}

impl Tuple {
    fn single_cell(cell: Cell) -> Self {
        Self{ contents: vec![cell] }
    }

    pub fn len(&self) -> usize {
        self.contents.len()
    }

    pub fn cell_at(&self, i: u32) -> Option<&Cell> {
        self.contents.get(i as usize)
    }

    pub fn contents(&self) -> &[Cell] {
        &self.contents
    }
}

#[derive(Clone)]
pub struct Cell {
    contents: Vec<u8>,
    kind: Type
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

    fn from_number(n: i32) -> Self {
        Self{ contents: Vec::from(n.to_be_bytes()), kind: Type::NUMBER }
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

impl QueryResults {
    fn count(n: i32) -> Self {
        let tuple = Tuple::single_cell(Cell::from_number(n));
        Self{ attributes: Rc::new(vec!["count".to_string()]), results: Rc::new(vec![tuple])}
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
