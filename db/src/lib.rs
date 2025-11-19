mod bytes;
mod event;
mod database;
pub mod dsl;
pub mod dump;
pub mod object;
pub mod parse;
pub mod plan;
pub mod schema;
pub mod tokenize;
pub mod tuple;
pub mod page;
mod heap;
pub mod interpret;
mod mapper;

#[cfg(test)]
pub mod test;

use core::fmt;
use std::cell::Cell;
use std::cmp::Ordering;
use std::collections::BTreeMap;
use std::error::Error;
use std::iter::zip;

use object::{Attribute, NamedAttribute as _};
use bytes::IntoBytes as _;

pub use crate::schema::Type;
pub use crate::database::Database;
pub use crate::dsl::{Query, Operator, Definition};
pub use crate::parse::{parse_definition, parse_query, parse_delete};
pub use crate::event::EventSource;

pub struct QueryResults {
    attributes: Vec<ResultAttribute>,
    results: Cell<Box<dyn Iterator<Item = Vec<u8>>>>,
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
    contents: Vec<u8>,
}

impl<'a> Tuple<'a> {
    pub fn len(&self) -> usize {
        self.contents.len()
    }

    pub fn is_empty(&self) -> bool {
        self.contents.is_empty()
    }

    pub fn attributes(&self) -> impl Iterator<Item = &ResultAttribute> {
        self.attributes.iter()
    }

    pub fn element(&self, name: &str) -> Option<Element<'_>> {
        let mut found_attr = None;
        let mut offset = 0usize;

        for attr in self.attributes {
            if attr == name {
                found_attr = Some(attr);
                break;
            } else {
                offset += attr.kind.size(&self.contents[offset..]);
            }
        }

        if let Some(attr) = found_attr {
            let size = attr.kind.size(&self.contents[offset..]);
            Some(Element { contents: &self.contents[offset..offset+size], kind: attr.kind })
        } else {
            None
        }
    }
}

#[derive(Clone, Eq, PartialEq)]
pub struct Element<'a> {
    contents: &'a [u8],
    kind: Type
}

impl<'a> Element<'a> {
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

impl fmt::Display for Element<'_> {
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
                let s = String::from_utf8(self.contents[1..].to_vec()).map_err(|_| fmt::Error)?;
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

impl TryFrom<Element<'_>> for i32 {
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

impl TryFrom<Element<'_>> for bool {
    type Error = ();

    fn try_from(value: Element) -> Result<Self, Self::Error> {
        (&value).try_into()
    }
}

impl TryFrom<&Element<'_>> for i32 {
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

impl TryFrom<&Element<'_>> for bool {
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

impl PartialOrd for Element<'_> {
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

impl fmt::Debug for Element<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{self}")
    }
}

impl QueryResults {
    pub(crate) fn single_number<T: Into<i32>>(name: &str, val: T) -> Self {
        let tuple = val.into().to_byte_vec();
        Self {
            attributes: vec![ResultAttribute { name: Box::from(name), kind: Type::NUMBER }],
            results: Cell::new(Box::new(std::iter::once(tuple))),
        }
    }

    pub(crate) fn empty() -> Self {
        Self { attributes: vec![], results: Cell::new(Box::new(std::iter::empty())) }
    }

    pub fn attributes(&self) -> &[ResultAttribute] {
        &self.attributes
    }

    pub fn tuples<'a>(&'a self) -> impl Iterator<Item = Tuple<'a>> {
        self.take_results().map(|contents| Tuple { attributes: &self.attributes, contents })
    }

    pub fn sort(self, attr: &str, order: SortOrder) -> Result<Self, SortError> {
        let mut sorted_contents = BTreeMap::new();
        for byte_tuple in self.take_results() {
            let tuple = Tuple { attributes: &self.attributes, contents: byte_tuple };
            let elem = tuple.element(attr).ok_or_else(|| SortError { missing_attribute: Box::from(attr) })?;
            sorted_contents.insert(elem.as_bytes().to_vec(), tuple.contents);
        }

        match order {
            SortOrder::SmallestFirst => Ok(Self {
                attributes: self.attributes,
                results: Cell::new(Box::new(sorted_contents.into_iter().map(|(_, v)| v))),
            }),
            SortOrder::LargestFirst => Ok(Self {
                attributes: self.attributes,
                results: Cell::new(Box::new(sorted_contents.into_iter().rev().map(|(_, v)| v))),
            }),
        }
    }

    fn take_results(&self) -> Box<dyn Iterator<Item = Vec<u8>>> {
        self.results.replace(Box::new(std::iter::empty()))
    }
}

#[derive(Debug)]
pub struct SortError {
    missing_attribute: Box<str>,
}

impl Error for SortError {}
impl fmt::Display for SortError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Missing attribute for sort: {}", self.missing_attribute)
    }
}

#[derive(Copy, Clone, Default)]
pub enum SortOrder { #[default] SmallestFirst, LargestFirst }

#[cfg(test)]
mod tests {
    use dsl::Insert;

    use super::*;

    #[test]
    fn test_sort_results() -> Result<(), Box<dyn Error>> {
        let mut db = Database::default();
        db.define(&Definition::relation("document")
                  .attribute("id", Type::NUMBER)
                  .attribute("size", Type::NUMBER)).unwrap();

        db.insert(&Insert::insert_into("document").element("id", 1).element("size", 123))?;
        db.insert(&Insert::insert_into("document").element("id", 2).element("size", 2))?;

        let result = db.execute_query(&Query::scan("document"))?;
        let sorted = result.sort("document.size", SortOrder::SmallestFirst).unwrap();
        let first = sorted.tuples().next().unwrap();
        assert_eq!(first.element("document.size").unwrap().to_string(), "2");

        let result = db.execute_query(&Query::scan("document"))?;
        let sorted = result.sort("document.size", SortOrder::LargestFirst).unwrap();
        let first = sorted.tuples().next().unwrap();
        assert_eq!(first.element("document.size").unwrap().to_string(), "123");

        Ok(())
    }
}
