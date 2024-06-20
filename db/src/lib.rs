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
    results: RefCell<Box<dyn Iterator<Item = Vec<Element>>>>,
}

#[derive(Debug)]
pub struct Tuple<'a> {
    attributes: &'a [String],
    contents: Vec<Element>,
}

impl<'a> Tuple<'a> {
    pub fn len(&self) -> usize {
        self.contents.len()
    }

    pub fn is_empty(&self) -> bool {
        self.contents.is_empty()
    }

    pub fn element(&self, name: &str) -> Option<&Element> {
        if let Some((i, _)) = self.attributes.iter().enumerate().find(|(_, n)| n == &name) {
            self.contents.get(i)
        } else {
            None
        }
    }

    pub fn contents(&self) -> &[Element] {
        &self.contents
    }
}

#[derive(Clone, Eq, PartialEq)]
pub struct Element {
    contents: Vec<u8>, // TODO: Borrow from the tuple
    kind: Type
}

impl Element {
    fn from_bytes(kind: Type, bytes: &[u8]) -> Element {
        if let Some(first) = bytes.first() {
            let firstchar = *first as char;
            if let Some(num) = firstchar.to_digit(8) {
                return Element{contents: vec![num as u8], kind}
            }
        }

        Element { contents: Vec::from(bytes), kind }
    }

    fn from_string(source: &str) -> Element {
        if let Result::Ok(number) = source.parse::<i32>() {
            Element { contents: Vec::from(number.to_be_bytes()), kind: Type::NUMBER }
        } else {
            Element { contents: Vec::from(source.as_bytes()), kind: Type::TEXT }
        }
    }

    pub fn as_bytes(&self) -> &[u8] {
        &self.contents
    }

    pub fn len(&self) -> usize {
        self.contents.len()
    }

    pub fn is_empty(&self) -> bool {
        self.contents.is_empty()
    }
}

impl fmt::Display for Element {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.kind {
            Type::NUMBER => {
                if let Ok(i) = <i32>::try_from(self) {
                    write!(f, "{i}")
                } else {
                    Err(fmt::Error)
                }
            },
            Type::TEXT => {
                let s = String::from_utf8(self.contents.clone()).map_err(|_| fmt::Error)?;
                write!(f, "{s}")
            },
            Type::BOOLEAN => {
                if let Ok(b) = <bool>::try_from(self) {
                    write!(f, "{b}")
                } else {
                    Err(fmt::Error)
                }
            },
            _ => write!(f, "no-display")
        }
    }
}

impl TryFrom<Element> for i32 {
    type Error = ();

    fn try_from(value: Element) -> Result<Self, Self::Error> {
        if value.kind == Type::NUMBER {
            <[u8; 4]>::try_from(value.contents)
                .map(i32::from_be_bytes)
                .map_err(|_| ())
        } else {
            Err(())
        }
    }
}

impl TryFrom<Element> for bool {
    type Error = ();

    fn try_from(value: Element) -> Result<Self, Self::Error> {
        (&value).try_into()
    }
}

impl TryFrom<&Element> for i32 {
    type Error = ();

    fn try_from(value: &Element) -> Result<Self, Self::Error> {
        if value.kind == Type::NUMBER {
            let bytes: [u8; 4] = slice_to_array(&value.contents)?;
            Ok(i32::from_be_bytes(bytes))
        } else {
            Err(())
        }
    }
}

fn slice_to_array<T: Default + Copy, const N: usize>(slice: &[T]) -> Result<[T; N], ()> {
    let mut arr = [T::default(); N];
    for i in 0..N {
        arr[i] = *slice.get(i).ok_or(())?;
    }

    Ok(arr)
}

impl TryFrom<&Element> for bool {
    type Error = ();

    fn try_from(value: &Element) -> Result<Self, Self::Error> {
        if value.kind == Type::BOOLEAN {
            value.contents.first()
                .map(|i| *i != 0)
                .ok_or(())
        } else {
            Err(())
        }
    }
}

impl From<i32> for Element {
    fn from(value: i32) -> Self {
        Self { contents: value.to_be_bytes().to_vec(), kind: Type::NUMBER }
    }
}

impl PartialOrd for Element {
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

impl fmt::Debug for Element {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{self}")
    }
}

impl QueryResults {
    pub(crate) fn single_number<T: Into<i32>>(name: &str, val: T) -> Self {
        let tuple = vec![Element::from(val.into())];
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
    results: &'a [Vec<Element>],
    pos: usize,
}

impl<'a> Iterator for Column<'a> {
    type Item = &'a Element;

    fn next(&mut self) -> Option<Self::Item> {
        let next = self.results.get(self.pos).and_then(|tuple| tuple.get(self.col_pos));
        self.pos += 1;
        next
    }
}

pub struct Tuples<'a> {
    attributes: &'a [String],
    contents: RefMut<'a, Box<dyn Iterator<Item = Vec<Element>>>>,
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
