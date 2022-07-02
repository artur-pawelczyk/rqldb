use crate::plan::Attribute;
use crate::schema::Type;

type ByteTuple = Vec<Vec<u8>>;

struct TupleSet<'a> {
    raw: &'a [ByteTuple],
    pos: usize,
}

struct FilteredTupleSet<'a> {
    source: TupleSet<'a>,
//    filter: 
}

impl<'a> TupleSet<'a> {
    fn from_object(raw: &'a [ByteTuple]) -> Self {
        Self { raw, pos: 0 }
    }
}

impl<'a> Iterator for TupleSet<'a> {
    type Item = Tuple<'a>;

    fn next(&mut self) -> Option<Tuple<'a>> {
        self.pos += 1;
        self.raw.get(self.pos - 1).map(|tuple| Tuple{ raw: tuple, rest: None })
    }
}

pub struct Tuple<'a> {
    raw: &'a ByteTuple,
    rest: Option<&'a ByteTuple>,
}

impl<'a> Tuple<'a> {
    pub(crate) fn from_bytes(raw: &'a ByteTuple) -> Self {
        Self{ raw, rest: None }
    }

    pub(crate) fn as_bytes(&self) -> &ByteTuple {
        self.raw
    }

    pub(crate) fn cell_by_attr(&self, attr: &Attribute) -> Cell {
        self.cell(attr.pos, attr.kind).unwrap()
    }

    pub(crate) fn cell(&self, pos: usize, kind: Type) -> Option<Cell> {
        if pos < self.raw.len() {
            Some(Cell{ raw: &self.raw[pos], kind })
        } else if self.rest.is_some() {
            let other = self.rest.unwrap();
            let other_pos = pos - self.raw.len();
            Some(Cell{ raw: &other[other_pos], kind })
        } else {
            None
        }

    }

    pub(crate) fn add_cells(mut self, other: &'a ByteTuple) -> Self {
        self.rest = Some(other);
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
}

impl<'a> ToString for Cell<'a> {
    fn to_string(&self) -> String {
        match self.kind {
            Type::NUMBER => {
                match <[u8; 4]>::try_from(self.raw.clone()) {
                    Ok(bytes) => i32::from_be_bytes(bytes).to_string(),
                    _ => panic!()
                }
            },
            Type::TEXT => std::str::from_utf8(&self.raw).unwrap().to_string(),
            _ => todo!()
        }
    }
}

impl<'a> TryFrom<Cell<'a>> for i32 {
    type Error = ();

    fn try_from(cell: Cell) ->  Result<i32, ()> {
        match cell.kind {
            Type::NUMBER => {
                match <[u8; 4]>::try_from(cell.raw.clone()) {
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_single_tuple() {
        let object = vec![tuple(&["1", "example"])];
        let result: Vec<Tuple> = TupleSet::from_object(&object).collect();
        let first = result.get(0).unwrap();
        assert_eq!(first.cell(0, Type::NUMBER).unwrap().to_string(), "1");
        assert_eq!(first.cell(1, Type::TEXT).unwrap().to_string(), "example");
    }

    #[test]
    fn test_filter() {
        let object = vec![
            tuple(&["1", "foo"]),
            tuple(&["2", "bar"]),
        ];

        let result: Vec<Tuple> = TupleSet::from_object(&object)
            .filter(|tuple| tuple.cell(0, Type::NUMBER).map(|cell| i32::try_from(cell).unwrap() == 1).unwrap_or(false))
            .collect();

        assert_eq!(result.get(0).unwrap().cell(1, Type::TEXT).unwrap().to_string(), "foo");
    }

    #[test]
    fn test_join()  {
        let object_1 = vec![
            tuple(&["1", "foo"]),
            tuple(&["2", "bar"]),
        ];
        let object_2 = vec![
            tuple(&["fizz"]),
        ];

        let result: Vec<Tuple> = TupleSet::from_object(&object_1).filter_map(|tuple| {
            object_2.get(0).map(|joiner_tuple| tuple.add_cells(joiner_tuple))
        }).collect();
        let first = result.get(0).unwrap();
        assert_eq!(i32::try_from(first.cell(0, Type::NUMBER).unwrap()), Ok(1));
        assert_eq!(first.cell(2, Type::TEXT).unwrap().to_string(), "fizz");
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
