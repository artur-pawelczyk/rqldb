use crate::plan::Attribute;
use crate::schema::Type;

#[derive(Debug)]
pub struct Tuple<'a> {
    raw: &'a [u8],
    attrs: &'a [Type],
    rest: Option<Box<Tuple<'a>>>,
}

impl<'a> Tuple<'a> {
    pub fn from_bytes(raw: &'a [u8], attrs: &'a [Type]) -> Self {
        Self{ raw, attrs, rest: None }
    }

    #[cfg(test)]
    pub(crate) fn as_bytes(&self) -> Vec<Vec<u8>> { 
        self.iter().map(|cell| cell.bytes().to_vec()).collect()
    }

    pub(crate) fn raw_bytes(&self) -> &[u8] {
        self.raw
    }

    pub(crate) fn cell_by_attr(&self, attr: &Attribute) -> Cell {
        self.cell(attr.pos()).unwrap()
    }

    pub(crate) fn cell(&self, pos: usize) -> Option<Cell> {
        if pos < self.attrs.len() {
            let kind = self.attrs.get(pos).copied()?;
            let start = self.offset(pos);
            let end = start + cell_len(kind, &self.raw[start..]);
            Some(Cell{ raw: &self.raw[start..end], kind })
        } else if let Some(rest) = &self.rest {
            let rest_pos = pos - self.attrs.len();
            rest.cell(rest_pos)
        } else {
            let rest_pos = pos - self.raw.len();
            let rest = self.rest.as_ref().unwrap();
            Some(rest.cell(rest_pos).unwrap())
        }
    }

    fn offset(&self, pos: usize) -> usize {
        let mut offset = 0usize;
        for attr in self.attrs.iter().take(pos) {
            let len = Cell{ raw: &self.raw[offset..], kind: *attr }.len();
            offset += len;
        }

        offset
    }

    pub(crate) fn iter(&self) -> CellIter {
        CellIter{ inner: Tuple{ raw: self.raw, attrs: self.attrs, rest: None } }
    }

    pub fn serialize(&self) -> TupleIter {
        fn size(val: usize) -> [u8; 4] {
            (val as u32).to_le_bytes()
        }

        let mut v: Vec<u8> = Vec::new();
        size(self.iter().count()).iter().for_each(|b| v.push(*b));
        for cell in self.iter() {
            size(cell.bytes().len()).iter().for_each(|b| v.push(*b));
            cell.bytes().iter().for_each(|b| v.push(*b));
        }

        TupleIter{ contents: v, pos: 0 }
    }

    pub(crate) fn add_cells(mut self, other: Tuple<'a>) -> Self {
        self.rest = Some(Box::new(other));
        self
    }
}

pub(crate) struct Cell<'a> {
    pub(crate) raw: &'a [u8],
    pub(crate) kind: Type,
}

impl<'a> Cell<'a> {
    fn new(kind: Type, bytes: &'a [u8]) -> Self {
        let len = cell_len(kind, bytes);
        Self{ kind, raw: &bytes[..len] }
    }

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
        cell_len(self.kind, self.raw)
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

pub(crate) struct CellIter<'a> {
    inner: Tuple<'a>,
}

impl<'a> Iterator for CellIter<'a> {
    type Item = Cell<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        if let Some(attr) = self.inner.attrs.first() {
            let cell = Cell::new(*attr, self.inner.raw);
            let offset = cell.len();
            self.inner = Tuple{ attrs: &self.inner.attrs[1..], raw: &self.inner.raw[offset..], rest: None };
            Some(cell)
        } else {
            None
        }
    }
}

impl<'a> ToString for Cell<'a> {
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

impl<'a> TryFrom<Cell<'a>> for i32 {
    type Error = ();

    fn try_from(cell: Cell) ->  Result<i32, ()> {
        match cell.kind {
            Type::NUMBER => {
                match <[u8; 4]>::try_from(cell.raw) {
                    Ok(bytes) => Ok(i32::from_be_bytes(bytes)),
                    _ => Err(()),
                }
            },
            _ => Err(()),
        }
    }
}

impl<'a> PartialEq for Cell<'a> {
    fn eq(&self, other: &Cell) -> bool {
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

fn cell_len(kind: Type, cell: &[u8]) -> usize {
    if cell.is_empty() {
        panic!()
    }
    match kind {
        Type::BOOLEAN => 1,
        Type::NUMBER => 4,
        Type::TEXT => cell[0] as usize + 1,
        _ => todo!(),
    }
}

#[cfg(test)]
mod tests {
    use std::iter::zip;

    use super::*;

    const TYPES_1: &[Type] = &[Type::NUMBER, Type::TEXT];
    const TYPES_2: &[Type] = &[Type::TEXT];

    #[test]
    fn test_tuple() {
        let object = vec![tuple(TYPES_1, &["1", "example"])];
        let tuple = Tuple::from_bytes(object.get(0).unwrap(), TYPES_1);
        assert_eq!(tuple.cell(0).unwrap().to_string(), "1");
        assert_eq!(tuple.cell(1).unwrap().to_string(), "example");
    }

    #[test]
    fn test_filter() {
        let object = vec![
            tuple(TYPES_1, &["1", "foo"]),
            tuple(TYPES_1, &["2", "bar"]),
        ];

        let result: Vec<Tuple> = object.iter().map(|bytes| Tuple::from_bytes(bytes, TYPES_1))
            .filter(|tuple| tuple.cell(0).map(|cell| i32::try_from(cell).unwrap() == 1).unwrap_or(false))
            .collect();

        assert_eq!(result.get(0).unwrap().cell(1).unwrap().to_string(), "foo");
    }

    #[test]
    fn test_add_cells()  {
        let object_1 = vec![
            tuple(TYPES_1, &["1", "foo"]),
            tuple(TYPES_1, &["2", "bar"]),
        ];
        let object_2 = vec![
            tuple(TYPES_2, &["fizz"]),
        ];

        let result: Vec<Tuple> = object_1.iter().map(|bytes| Tuple::from_bytes(bytes, TYPES_1))
            .filter_map(|tuple| {
                object_2.get(0).map(|joiner_tuple| tuple.add_cells(Tuple::from_bytes(joiner_tuple, TYPES_2)))
            }).collect();
        let first = result.get(0).unwrap();
        assert_eq!(i32::try_from(first.cell(0).unwrap()), Ok(1));
        assert_eq!(first.cell(2).unwrap().to_string(), "fizz");
    }

    fn tuple(types: &[Type], values: &[&str]) -> Vec<u8> {
        let mut tuple = Vec::new();
        for (kind, value) in zip(types, values) {
            let cell = Cell{ raw: &cell(value), kind: *kind };
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
