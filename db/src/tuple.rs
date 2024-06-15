use core::fmt;

use crate::object::{Attribute, IndexedObject, NamedAttribute as _};
use crate::schema::Type;

pub(crate) trait PositionalAttribute {
    fn pos(&self) -> usize;
    fn object_id(&self) -> Option<usize> {
        None
    }
}

impl PositionalAttribute for usize {
    fn pos(&self) -> usize {
        *self
    }
}

#[derive(Debug)]
pub(crate) struct Tuple<'a> {
    raw: &'a [u8],
    attrs: &'a [Attribute],
    source_id: usize,
    rest: Option<Box<Tuple<'a>>>,
}

impl<'a> Tuple<'a> {
    pub(crate) fn from_bytes(raw: &'a [u8], attrs: &'a [Attribute]) -> Self {
        Self { raw, attrs, rest: None, source_id: 0 }
    }

    pub(crate) fn with_object(raw: &'a [u8], obj: &'a IndexedObject) -> Self {
        Self { raw, attrs: &obj.attrs, source_id: obj.id(), rest: None }
    }

    pub(crate) fn raw_bytes(&self) -> &[u8] {
        self.raw
    }

    pub(crate) fn element(&self, attr: &impl PositionalAttribute) -> Option<Element> {
        let pos = attr.pos();
        if attr.object_id().map(|id| id != self.source_id).unwrap_or(false) {
            if let Some(rest) = &self.rest {
                return rest.element(attr);
            }
        }

        let attr = self.attrs.get(pos)?;
        let start = self.offset(pos);
        let end = start + element_len(attr.kind(), &self.raw[start..]);
        Some(Element { raw: &self.raw[start..end], kind: attr.kind() })
    }

    fn offset(&self, pos: usize) -> usize {
        let mut offset = 0usize;
        for attr in self.attrs.iter().take(pos) {
            let len = Element{ raw: &self.raw[offset..], kind: attr.kind() }.len();
            offset += len;
        }

        offset
    }

    pub(crate) fn extend(mut self, other: Tuple<'a>) -> Self {
        self.rest = Some(Box::new(other));
        self
    }
}

#[derive(Debug)]
pub(crate) struct Element<'a> {
    pub(crate) raw: &'a [u8],
    pub(crate) kind: Type,
}

impl<'a> Element<'a> {
    pub fn bytes(&self) -> &[u8] {
        &self.raw[self.offset()..self.len()]
    }

    pub fn as_number(&self) -> Option<i32> {
        match self.kind {
            Type::NUMBER => {
                let bytes: Result<[u8; 4], _> = self.raw.try_into();
                match bytes {
                    Ok(x) => Some(i32::from_be_bytes(x)),
                    _ => None
                }
            },
            _ => None
        }
    }

    fn len(&self) -> usize {
        element_len(self.kind, self.raw)
    }

    fn offset(&self) -> usize {
        if self.kind == Type::TEXT {
            1
        } else {
            0
        }
    }

    #[cfg(test)]
    pub(crate) fn serialize(&self) -> Vec<u8> {
        self.raw.to_vec()
    }
}

impl fmt::Display for Element<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.kind {
            Type::NUMBER => {
                match <[u8; 4]>::try_from(self.raw) {
                    Ok(bytes) => write!(f, "{}", i32::from_be_bytes(bytes)),
                    _ => Err(fmt::Error),
                }
            },
            Type::TEXT => {
                if let Ok(s) = std::str::from_utf8(&self.raw[1..]) {
                    write!(f, "{s}")
                } else {
                    Err(fmt::Error)
                }
            },
            _ => Err(fmt::Error)
        }
    }
}

impl<'a> TryFrom<Element<'a>> for i32 {
    type Error = ();

    fn try_from(e: Element) ->  Result<i32, ()> {
        match e.kind {
            Type::NUMBER => {
                match <[u8; 4]>::try_from(e.raw) {
                    Ok(bytes) => Ok(i32::from_be_bytes(bytes)),
                    _ => Err(()),
                }
            },
            _ => Err(()),
        }
    }
}

impl<'a> PartialEq for Element<'a> {
    fn eq(&self, other: &Element) -> bool {
        self.raw == other.raw
    }

}

pub struct TupleIter {
    contents: Vec<u8>,
    pos: usize,
}

impl Iterator for TupleIter {
    type Item = u8;

    fn next(&mut self) -> Option<Self::Item> {
        self.pos += 1;
        self.contents.get(self.pos - 1).cloned()
    }
}

fn element_len(kind: Type, elem: &[u8]) -> usize {
    if elem.is_empty() {
        panic!()
    }

    match kind {
        Type::BOOLEAN => 1,
        Type::NUMBER => 4,
        Type::TEXT => elem[0] as usize + 1,
        _ => todo!(),
    }
}

#[cfg(test)]
mod tests {
    use std::iter::zip;
    use lazy_static::lazy_static;

    use crate::schema::Schema;

    use super::*;

    lazy_static! {
        static ref SCHEMA: Schema = {
            let mut schema = Schema::default();
            schema.create_table("first")
                .column("id", Type::NUMBER)
                .column("content", Type::TEXT)
                .add();
            schema.create_table("second")
                .column("content", Type::TEXT)
                .add();
            schema
        };

        static ref ATTRIBUTES_1: Vec<Attribute> = {
            SCHEMA.find_relation("first").unwrap().attributes().map(Into::into).collect()
        };

        static ref ATTRIBUTES_2: Vec<Attribute> = {
            SCHEMA.find_relation("second").unwrap().attributes().map(Into::into).collect()
        };
    }

    #[test]
    fn test_tuple() {
        let object = [tuple(&ATTRIBUTES_1, &["1", "example"])];
        let tuple = Tuple::from_bytes(object.get(0).unwrap(), &ATTRIBUTES_1);
        assert_eq!(tuple.element(&0).unwrap().to_string(), "1");
        assert_eq!(tuple.element(&1).unwrap().to_string(), "example");
    }

    #[test]
    fn test_filter() {
        let object = [
            tuple(&ATTRIBUTES_1, &["1", "foo"]),
            tuple(&ATTRIBUTES_1, &["2", "bar"]),
        ];

        let result: Vec<Tuple> = object.iter().map(|bytes| Tuple::from_bytes(bytes, &ATTRIBUTES_1))
            .filter(|tuple| tuple.element(&0).map(|cell| i32::try_from(cell).unwrap() == 1).unwrap_or(false))
            .collect();

        assert_eq!(result.get(0).unwrap().element(&1).unwrap().to_string(), "foo");
    }

    #[test]            
    fn test_add_cells()  {
        let object_1 = [
            tuple(&ATTRIBUTES_1, &["1", "foo"]),
            tuple(&ATTRIBUTES_1, &["2", "bar"]),
        ];
        let object_2 = [
            tuple(&ATTRIBUTES_2, &["fizz"]),
        ];

        let result: Vec<Tuple> = object_1.iter().map(|bytes| Tuple::from_bytes(bytes, &ATTRIBUTES_1))
            .filter_map(|tuple| {
                object_2.get(0).map(|joiner_tuple| tuple.extend(Tuple::from_bytes(joiner_tuple, &ATTRIBUTES_2)))
            }).collect();
        let first = result.get(0).unwrap();
        assert_eq!(i32::try_from(first.element(&ATTRIBUTES_1[0]).unwrap()), Ok(1));
        assert_eq!(first.element(&ATTRIBUTES_2[0]).unwrap().to_string(), "fizz");
    }

    fn tuple(attrs: &[Attribute], values: &[&str]) -> Vec<u8> {
        let mut tuple = Vec::new();
        for (attr, value) in zip(attrs, values) {
            let cell = Element{ raw: &cell(value), kind: attr.kind };
            tuple.append(&mut cell.serialize());
        }

        tuple
    }

    fn cell(s: &str) -> Vec<u8> {
        if let Result::Ok(num) = s.parse::<i32>() {
            Vec::from(num.to_be_bytes())
        } else {
            let mut v = vec![s.len() as u8];
            s.as_bytes().iter().for_each(|c| v.push(*c));
            v
        }
    }
}
