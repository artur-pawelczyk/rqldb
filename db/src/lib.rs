 pub mod db;
pub mod dsl;
pub mod parse;
pub mod plan;
pub mod schema;
pub mod tokenize;
pub mod tuple;
pub mod object;
pub mod dump;
mod bytes;

use core::fmt;
use std::cell::{RefCell, RefMut};
use std::cmp::Ordering;
use std::iter::zip;

use object::{Attribute, NamedAttribute as _};
use bytes::{IntoBytes as _};

pub use crate::schema::Type;
pub use crate::db::Database;
pub use crate::dsl::{Query, Operator, Command};
pub use crate::parse::{parse_command, parse_query};
pub use crate::object::RawObjectView;

pub struct QueryResults {
    attributes: Vec<ResultAttribute>,
    results: RefCell<Box<dyn Iterator<Item = Vec<Vec<u8>>>>>,
}

#[derive(Debug, PartialEq, Eq)]
pub struct ResultAttribute {
    name: Box<str>,
    kind: Type,
}

impl ResultAttribute {
    pub fn name(&self) -> &str {
        self.name.as_ref()
    }
}

impl From<&Attribute> for ResultAttribute {
    fn from(attr: &Attribute) -> Self {
        Self {
            name: Box::from(attr.name()),
            kind: attr.kind(),
        }
    }
}

impl PartialEq<str> for ResultAttribute {
    fn eq(&self, s: &str) -> bool {
        self.name.as_ref() == s
    }
}

#[derive(Debug)]
pub struct Tuple<'a> {
    attributes: &'a [ResultAttribute],
    contents: Vec<Vec<u8>>, // TODO: Use Vec<u8>
}

impl<'a> Tuple<'a> {
    pub fn len(&self) -> usize {
        self.contents.len()
    }

    pub fn is_empty(&self) -> bool {
        self.contents.is_empty()
    }

    pub fn element(&self, name: &str) -> Option<Element> {
        if let Some((i, attr)) = self.attributes.iter().enumerate().find(|(_, n)| *n == name) {
            self.contents.get(i).map(|bytes| Element { contents: bytes.to_vec(), kind: attr.kind })
        } else {
            None
        }
    }
}

#[derive(Clone, Eq, PartialEq)]
pub struct Element {
    contents: Vec<u8>, // TODO: Borrow from the tuple
    kind: Type
}

impl Element {
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
    for (o, i) in zip(arr.iter_mut(), slice.iter()) {
        *o = *i;
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
        let tuple = vec![val.into().to_byte_vec()];
        Self {
            attributes: vec![ResultAttribute { name: Box::from(name), kind: Type::NUMBER }],
            results: RefCell::new(Box::new(std::iter::once(tuple)))
        }
    }

    pub(crate) fn empty() -> Self {
        Self{ attributes: vec![], results: RefCell::new(Box::new(std::iter::empty())) }
    }

    pub fn attributes(&self) -> &[ResultAttribute] {
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
    attributes: &'a [ResultAttribute],
    contents: RefMut<'a, Box<dyn Iterator<Item = Vec<Vec<u8>>>>>,
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
