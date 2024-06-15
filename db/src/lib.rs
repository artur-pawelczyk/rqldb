 pub mod db;
pub mod dsl;
pub mod parse;
pub mod plan;
pub mod schema;
pub mod tokenize;
pub mod tuple;
pub mod object;
pub mod dump;

use core::fmt;
use std::cell::{RefCell, RefMut};
use std::cmp::Ordering;

pub use crate::schema::Type;
pub use crate::db::Database;
pub use crate::dsl::{Query, Operator, Command};
pub use crate::parse::{parse_command, parse_query};
pub use crate::object::RawObjectView;

pub struct QueryResults {
    attributes: Vec<String>,
    results: RefCell<Box<dyn Iterator<Item = Vec<Cell>>>>,
}

#[derive(Debug)]
pub struct Tuple<'a> {
    attributes: &'a [String],
    contents: Vec<Cell>,
}

impl<'a> Tuple<'a> {
    pub fn len(&self) -> usize {
        self.contents.len()
    }

    pub fn is_empty(&self) -> bool {
        self.contents.is_empty()
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
        &self.contents
    }
}

#[derive(Clone, Eq, PartialEq)]
pub struct Cell {
    contents: Vec<u8>,
    kind: Type
}

impl Cell {
    fn from_bytes(kind: Type, bytes: &[u8]) -> Cell {
        if let Some(first) = bytes.first() {
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

    
    fn from_i32(n: i32) -> Self {
        Self{ contents: Vec::from(n.to_be_bytes()), kind: Type::NUMBER }
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

    pub fn as_bool(&self) -> Option<bool> {
        if self.kind == Type::BOOLEAN {
            self.contents.first().map(|i| *i != 0)
        } else {
            None
        }
    }

    pub fn len(&self) -> usize {
        self.contents.len()
    }

    pub fn is_empty(&self) -> bool {
        self.contents.is_empty()
    }
}

impl fmt::Display for Cell {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.kind {
            Type::NUMBER => {
                write!(f, "{}", self.as_number().ok_or(fmt::Error)?)
            },
            Type::TEXT => {
                let s = String::from_utf8(self.contents.clone()).map_err(|_| fmt::Error)?;
                write!(f, "{s}")
            },
            Type::BOOLEAN => {
                if let Some(b) = self.as_bool() {
                    write!(f, "{b}")
                } else {
                    Err(fmt::Error)
                }
            },
            _ => write!(f, "no-display")
        }
    }
}

impl<T: Into<i32>> From<T> for Cell {
    fn from(num: T) -> Self {
        Self::from_i32(num.into())
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
        write!(f, "{self}")
    }
}

impl QueryResults {
    pub(crate) fn single_number<T: Into<i32>>(name: &str, val: T) -> Self {
        let tuple = vec![Cell::from(val)];
        Self{ attributes: vec![name.to_string()], results: RefCell::new(Box::new(std::iter::once(tuple))) }
    }

    pub(crate) fn empty() -> Self {
        Self{ attributes: vec![], results: RefCell::new(Box::new(std::iter::empty())) }
    }

    pub fn attributes(&self) -> &[String] {
        &self.attributes
    }

    pub fn tuples(&self) -> Tuples {
        Tuples {
            attributes: &self.attributes,
            contents: self.results.borrow_mut(),
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

pub struct Tuples<'a> {
    attributes: &'a [String],
    contents: RefMut<'a, Box<dyn Iterator<Item = Vec<Cell>>>>,
}

impl<'a> Iterator for Tuples<'a> {
    type Item = Tuple<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        self.contents.next().map(|cells| {
            debug_assert!(self.attributes.len() == cells.len());
            Tuple { attributes: self.attributes, contents: cells }
        })
    }
}
