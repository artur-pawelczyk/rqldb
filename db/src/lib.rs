 pub mod db;
pub mod dsl;
pub mod parse;
pub mod plan;
pub mod schema;
pub mod tokenize;
pub mod tuple;
pub mod object;

use std::fmt;
use std::cmp::Ordering;

pub use crate::schema::Type;
pub use crate::db::Database;
pub use crate::dsl::{Query, Operator, Command};
pub use crate::parse::{parse_command, parse_query};

#[derive(Debug)]
pub struct QueryResults {
    attributes: Vec<String>,
    results: Vec<Vec<Cell>>
}

#[derive(Debug)]
pub struct Tuple<'a> {
    attributes: &'a [String],
    contents: &'a [Cell],
}

impl<'a> Tuple<'a> {
    pub fn len(&self) -> usize {
        self.contents.len()
    }

    pub fn cell_at(&self, i: u32) -> Option<&Cell> {
        self.contents.get(i as usize)
    }

    pub fn cell_by_name(&self, name: &str) -> Option<&Cell> {
        if let Some((i, _)) = self.attributes.iter().enumerate().find(|(_, n)| n == &name) {
            self.contents.get(i)
        } else {
            None
        }
    }

    pub fn contents(&self) -> &[Cell] {
        self.contents
    }
}

#[derive(Clone, PartialEq)]
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

    
    fn from_number(n: u32) -> Self {
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

    pub fn into_bytes(self) -> Vec<u8> {
        self.contents
    }

    pub fn as_bytes(&self) -> &[u8] {
        &self.contents
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

    pub fn len(&self) -> usize {
        self.contents.len()
    }
}

impl PartialOrd for Cell {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        if self.kind == other.kind {
            if self.contents > other.contents {
                Some(Ordering::Greater)
            } else if self.contents == other.contents {
                Some(Ordering::Equal)
            } else {
                Some(Ordering::Less)
            }
        } else {
            None
        }
    }
}

impl fmt::Debug for Cell {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.as_string())
    }
}

impl QueryResults {
    pub(crate) fn count(n: u32) -> Self {
        let tuple = vec![Cell::from_number(n)];
        Self{ attributes: vec!["count".to_string()], results: vec![tuple]}
    }

    pub(crate) fn empty() -> Self {
        Self{ attributes: vec![], results: vec![] }
    }

    pub fn size(&self) -> u32 {
        self.results.len() as u32
    }

    pub fn attributes(&self) -> &Vec<String> {
        &self.attributes
    }

    pub fn results(&self) -> Vec<Tuple> {
        self.results.iter().map(|x| Tuple{ attributes: &self.attributes, contents: x}).collect()
    }

    pub fn tuple_at(&self, i: i32) -> Option<Tuple> {
        self.results.get(i as usize).map(|cells| Tuple{ attributes: &self.attributes, contents: cells })
    }

    pub fn columns(&self) -> Vec<Column> {
        (0..self.attributes.len()).map(|col_pos| Column{ results: &self.results, col_pos, pos: 0}).collect()
    }

    pub fn column(&self, name: &str) -> Option<Column> {
        if let Some(col_pos) = self.attributes().iter().enumerate().find(|(_, attr)| attr == &name).map(|(i, _)| i) {
            Some(Column{ results: &self.results, col_pos, pos: 0 })
        } else {
            None
        }
    }
}

pub struct Column<'a> {
    col_pos: usize,
    results: &'a [Vec<Cell>],
    pos: usize,
}

impl<'a> Iterator for Column<'a> {
    type Item = &'a Cell;

    fn next(&mut self) -> Option<Self::Item> {
        let next = self.results.get(self.pos).and_then(|tuple| tuple.get(self.col_pos));
        self.pos += 1;
        next
    }
}
