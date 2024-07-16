mod bytes;
mod event;
pub mod db;
pub mod dsl;
pub mod dump;
pub mod object;
pub mod parse;
pub mod plan;
pub mod schema;
pub mod tokenize;
pub mod tuple;

use core::fmt;
use std::cmp::Ordering;
use std::collections::BTreeMap;
use std::error::Error;
use std::iter::zip;

use object::{Attribute, NamedAttribute as _};
use bytes::IntoBytes as _;

pub use crate::schema::Type;
pub use crate::db::Database;
pub use crate::dsl::{Query, Operator, Definition};
pub use crate::parse::{parse_definition, parse_query};
pub use crate::object::RawObjectView;
pub use crate::event::EventSource;

pub struct QueryResults {
    attributes: Vec<ResultAttribute>,
    results: Vec<Vec<u8>>,
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
    contents: &'a [u8],
}

impl<'a> Tuple<'a> {
    pub fn len(&self) -> usize {
        self.contents.len()
    }

    pub fn is_empty(&self) -> bool {
        self.contents.is_empty()
    }

    pub fn element(&self, name: &str) -> Option<Element> {
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
            results: vec![tuple],
        }
    }

    pub(crate) fn empty() -> Self {
        Self { attributes: vec![], results: Vec::new() }
    }

    pub fn attributes(&self) -> &[ResultAttribute] {
        &self.attributes
    }

    pub fn tuples(&self) -> impl Iterator<Item = Tuple> {
        Tuples {
            attributes: &self.attributes,
            contents: &self.results
        }
    }

    pub fn sort(self, attr: &str, order: SortOrder) -> Result<Self, SortError> {
        let mut sorted_contents = BTreeMap::new();
        for byte_tuple in self.results.into_iter() {
            let tuple = Tuple { attributes: &self.attributes, contents: &byte_tuple };
            let elem = tuple.element(attr).ok_or_else(|| SortError { missing_attribute: Box::from(attr) })?;
            sorted_contents.insert(elem.as_bytes().to_vec(), byte_tuple);
        }

        match order {
            SortOrder::SmallestFirst => Ok(Self {
                attributes: self.attributes,
                results: sorted_contents.into_iter().map(|(_, v)| v).collect()
            }),
            SortOrder::LargestFirst => Ok(Self {
                attributes: self.attributes,
                results: sorted_contents.into_iter().rev().map(|(_, v)| v).collect()
            }),
        }
    }
}

struct Tuples<'a> {
    attributes: &'a [ResultAttribute],
    contents: &'a [Vec<u8>],
}

impl<'a> Iterator for Tuples<'a> {
    type Item = Tuple<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        if let Some(bytes) = self.contents.get(0) {
            let tuple = Tuple { attributes: self.attributes, contents: bytes };
            self.contents = &self.contents[1..];
            Some(tuple)
        } else {
            None
        }
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
    use self::object::TempObject;
    use self::schema::Schema;

    use super::*;

    #[test]
    fn test_sort_results() {
        let mut schema = Schema::default();
        schema.create_table("document")
            .column("id", Type::NUMBER)
            .column("size", Type::NUMBER)
            .add();

        let mut obj = TempObject::from_relation(schema.find_relation("document").unwrap());
        obj.push_str(&["1", "123"]);
        obj.push_str(&["2", "2"]);

        let result = QueryResults {
            attributes: obj.attributes().map(ResultAttribute::from).collect(),
            results: obj.iter().map(|tuple| tuple.raw_bytes().to_vec()).collect(),
        };

        let sorted = result.sort("document.size", SortOrder::SmallestFirst).unwrap();
        let first = sorted.tuples().next().unwrap();
        assert_eq!(first.element("document.size").unwrap().to_string(), "2");

        let result = QueryResults {
            attributes: obj.attributes().map(ResultAttribute::from).collect(),
            results: obj.iter().map(|tuple| tuple.raw_bytes().to_vec()).collect(),
        };

        let sorted = result.sort("document.size", SortOrder::LargestFirst).unwrap();
        let first = sorted.tuples().next().unwrap();
        assert_eq!(first.element("document.size").unwrap().to_string(), "123");
    }
}
