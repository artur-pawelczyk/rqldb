use crate::object::{Attribute, NamedAttribute as _};
use crate::schema::Type;

pub(crate) trait PositionalAttribute {
    fn pos(&self) -> usize;
}

impl PositionalAttribute for usize {
    fn pos(&self) -> usize {
        *self
    }
}

#[derive(Debug)]
pub struct Tuple<'a> {
    raw: &'a [u8],
    attrs: &'a [Attribute],
    rest: Option<Box<Tuple<'a>>>,
}

impl<'a> Tuple<'a> {
    pub fn from_bytes(raw: &'a [u8], attrs: &'a [Attribute]) -> Self {
        Self{ raw, attrs, rest: None }
    }

    #[cfg(test)]
    pub(crate) fn as_bytes(&self) -> Vec<Vec<u8>> { 
        self.iter().map(|e| e.bytes().to_vec()).collect()
    }

    pub(crate) fn raw_bytes(&self) -> &[u8] {
        self.raw
    }

    pub(crate) fn element(&self, attr: &impl PositionalAttribute) -> Option<Element> {
        let pos = attr.pos();
        if pos < self.attrs.len() {
            let attr = self.attrs.get(pos)?;
            let start = self.offset(pos);
            let end = start + element_len(attr.kind(), &self.raw[start..]);
            Some(Element{ raw: &self.raw[start..end], name: attr.name(), kind: attr.kind() })
        } else if let Some(rest) = &self.rest {
            let rest_pos = pos - self.attrs.len();
            rest.element(&rest_pos)
        } else {
            let rest_pos = pos - self.raw.len();
            let rest = self.rest.as_ref().unwrap();
            Some(rest.element(&rest_pos).unwrap())
        }
    }

    fn offset(&self, pos: usize) -> usize {
        let mut offset = 0usize;
        for attr in self.attrs.iter().take(pos) {
            let len = Element{ raw: &self.raw[offset..], name: attr.name(), kind: attr.kind() }.len();
            offset += len;
        }

        offset
    }

    pub(crate) fn iter(&self) -> ElementIter {
        ElementIter{ inner: Tuple{ raw: self.raw, attrs: self.attrs, rest: None } }
    }

    pub fn serialize(&self) -> TupleIter {
        fn size(val: usize) -> [u8; 4] {
            (val as u32).to_le_bytes()
        }

        let mut v: Vec<u8> = Vec::new();
        size(self.iter().count()).iter().for_each(|b| v.push(*b));
        for elem in self.iter() {
            size(elem.bytes().len()).iter().for_each(|b| v.push(*b));
            elem.bytes().iter().for_each(|b| v.push(*b));
        }

        TupleIter{ contents: v, pos: 0 }
    }

    pub(crate) fn extend(mut self, other: Tuple<'a>) -> Self {
        self.rest = Some(Box::new(other));
        self
    }
}

#[derive(Debug)]
pub(crate) struct Element<'a> {
    pub(crate) raw: &'a [u8],
    pub(crate) name: &'a str,
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

pub(crate) struct ElementIter<'a> {
    inner: Tuple<'a>,
}

impl<'a> Iterator for ElementIter<'a> {
    type Item = Element<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        if let Some(attr) = self.inner.attrs.first() {
            let len = element_len(attr.kind(), self.inner.raw);
            let elem = Element { raw: &self.inner.raw[..len], name: attr.name(), kind: attr.kind() };
            self.inner = Tuple{ attrs: &self.inner.attrs[1..], raw: &self.inner.raw[len..], rest: None };
            Some(elem)
        } else {
            None
        }
    }
}

impl<'a> ToString for Element<'a> {
    fn to_string(&self) -> String {
        match self.kind {
            Type::NUMBER => {
                match <[u8; 4]>::try_from(self.raw) {
                    Ok(bytes) => i32::from_be_bytes(bytes).to_string(),
                    _ => panic!("Error parsing number: {:?}", self.raw)
                }
            },
            Type::TEXT => std::str::from_utf8(&self.raw[1..]).unwrap().to_string(),
            _ => todo!()
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

    use super::*;

    lazy_static! {
        static ref ATTRIBUTES_1: Vec<Attribute> = vec![
            Attribute { pos: 0, name: Box::from("id"), kind: Type::NUMBER, reference: None },
            Attribute { pos: 1, name: Box::from("content"), kind: Type::TEXT, reference: None }
        ];
        static ref ATTRIBUTES_2: Vec<Attribute> = vec![
            Attribute { pos: 0, name: Box::from("id"), kind: Type::TEXT, reference: None }
        ];
    }

    #[test]
    fn test_tuple() {
        let object = vec![tuple(&ATTRIBUTES_1, &["1", "example"])];
        let tuple = Tuple::from_bytes(object.get(0).unwrap(), &ATTRIBUTES_1);
        assert_eq!(tuple.element(&0).unwrap().to_string(), "1");
        assert_eq!(tuple.element(&1).unwrap().to_string(), "example");
    }

    #[test]
    fn test_filter() {
        let object = vec![
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
        let object_1 = vec![
            tuple(&ATTRIBUTES_1, &["1", "foo"]),
            tuple(&ATTRIBUTES_1, &["2", "bar"]),
        ];
        let object_2 = vec![
            tuple(&ATTRIBUTES_2, &["fizz"]),
        ];

        let result: Vec<Tuple> = object_1.iter().map(|bytes| Tuple::from_bytes(bytes, &ATTRIBUTES_1))
            .filter_map(|tuple| {
                object_2.get(0).map(|joiner_tuple| tuple.extend(Tuple::from_bytes(joiner_tuple, &ATTRIBUTES_2)))
            }).collect();
        let first = result.get(0).unwrap();
        assert_eq!(i32::try_from(first.element(&0).unwrap()), Ok(1));
        assert_eq!(first.element(&2).unwrap().to_string(), "fizz");
    }

    fn tuple(attrs: &[Attribute], values: &[&str]) -> Vec<u8> {
        let mut tuple = Vec::new();
        for (attr, value) in zip(attrs, values) {
            let cell = Element{ raw: &cell(value), kind: attr.kind, name: attr.name() };
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
