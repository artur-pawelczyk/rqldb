use crate::plan::Attribute;
use crate::schema::Type;

type ByteTuple = Vec<Vec<u8>>;

#[derive(Debug)]
pub struct Tuple<'a> {
    raw: &'a ByteTuple,
    attrs: &'a [Type],
    rest: Option<Box<Tuple<'a>>>,
}

impl<'a> Tuple<'a> {
    pub fn from_bytes(raw: &'a ByteTuple, attrs: &'a [Type]) -> Self {
        Self{ raw, attrs, rest: None }
    }

    pub(crate) fn as_bytes(&self) -> &ByteTuple { 
       self.raw
    }

    pub(crate) fn cell_by_attr(&self, attr: &Attribute) -> Cell {
        if attr.pos() >= self.raw.len() {
            println!("looking for rest: {:?}", self.rest);
        }

        self.cell(attr.pos()).unwrap()
    }

    pub(crate) fn cell(&self, pos: usize) -> Option<Cell> {
        if pos < self.raw.len() {
            let kind = self.attrs.get(pos).map(|x| *x)?;
            Some(Cell{ raw: &self.raw[pos], kind })
        } else if let Some(rest) = &self.rest {
            let rest_pos = pos - self.raw.len();
            rest.cell(rest_pos)
        } else {
            let rest_pos = pos - self.raw.len();
            let rest = self.rest.as_ref().unwrap();
            Some(rest.cell(rest_pos).unwrap())
        }
    }

    pub fn len(&self) -> usize {
        self.raw.len() + if let Some(rest) = &self.rest { rest.len() } else { 0 }
    }

    pub fn serialize(&self) -> TupleIter {
        fn size(val: usize) -> [u8; 4] {
            (val as u32).to_le_bytes()
        }

        let mut v: Vec<u8> = Vec::new();
        size(self.raw.len()).iter().for_each(|b| v.push(*b));
        for cell in self.raw {
            size(cell.len()).iter().for_each(|b| v.push(*b));
            cell.iter().for_each(|b| v.push(*b));
        }

        TupleIter{ contents: v, pos: 0 }
    }

    pub(crate) fn add_cells(mut self, other: Tuple<'a>) -> Self {
        self.rest = Some(Box::new(other));
        self
    }
}

pub(crate) struct Cell<'a> {
    raw: &'a [u8],
    kind: Type,
}

impl<'a> Cell<'a> {
    pub fn bytes(&self) -> &[u8] {
        self.raw
    }

    pub fn as_number(&self) -> Option<i32> {
        match self.kind {
            Type::NUMBER => {
                let bytes: Result<[u8; 4], _> = self.raw.clone().try_into();
                match bytes {
                    Ok(x) => Some(i32::from_be_bytes(x)),
                    _ => None
                }
            },
            _ => None
        }
    }
}

impl<'a> ToString for Cell<'a> {
    fn to_string(&self) -> String {
        match self.kind {
            Type::NUMBER => {
                match <[u8; 4]>::try_from(self.raw) {
                    Ok(bytes) => i32::from_be_bytes(bytes).to_string(),
                    _ => panic!()
                }
            },
            Type::TEXT => std::str::from_utf8(self.raw).unwrap().to_string(),
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

#[cfg(test)]
mod tests {
    use super::*;

    const TYPES_1: &'static [Type] = &[Type::NUMBER, Type::TEXT];
    const TYPES_2: &'static [Type] = &[Type::TEXT];

    #[test]
    fn test_tuple() {
        let object = vec![tuple(&["1", "example"])];
        let tuple = Tuple::from_bytes(object.get(0).unwrap(), TYPES_1);
        assert_eq!(tuple.cell(0).unwrap().to_string(), "1");
        assert_eq!(tuple.cell(1).unwrap().to_string(), "example");
    }

    #[test]
    fn test_filter() {
        let object = vec![
            tuple(&["1", "foo"]),
            tuple(&["2", "bar"]),
        ];

        let result: Vec<Tuple> = object.iter().map(|bytes| Tuple::from_bytes(bytes, TYPES_1))
            .filter(|tuple| tuple.cell(0).map(|cell| i32::try_from(cell).unwrap() == 1).unwrap_or(false))
            .collect();

        assert_eq!(result.get(0).unwrap().cell(1).unwrap().to_string(), "foo");
    }

    #[test]
    fn test_add_cells()  {
        let object_1 = vec![
            tuple(&["1", "foo"]),
            tuple(&["2", "bar"]),
        ];
        let object_2 = vec![
            tuple(&["fizz"]),
        ];

        let result: Vec<Tuple> = object_1.iter().map(|bytes| Tuple::from_bytes(bytes, TYPES_1))
            .filter_map(|tuple| {
                object_2.get(0).map(|joiner_tuple| tuple.add_cells(Tuple::from_bytes(joiner_tuple, TYPES_2)))
            }).collect();
        let first = result.get(0).unwrap();
        assert_eq!(i32::try_from(first.cell(0).unwrap()), Ok(1));
        assert_eq!(first.cell(2).unwrap().to_string(), "fizz");
    }

    fn tuple(cells: &[&str]) -> ByteTuple {
        cells.iter().map(|s| cell(s)).collect()
    }

    fn cell(s: &str) -> Vec<u8> {
        if let Result::Ok(num) = s.parse::<i32>() {
            Vec::from(num.to_be_bytes())
        } else {
            Vec::from(s.as_bytes())
        }
    }
}
