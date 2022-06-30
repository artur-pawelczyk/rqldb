use crate::plan::Attribute;
use crate::schema::Type;

type ByteTuple = Vec<Vec<u8>>;

struct TupleSet<'a> {
    raw: &'a [ByteTuple],
    pos: usize,
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

struct Tuple<'a> {
    raw: &'a ByteTuple,
    rest: Option<&'a ByteTuple>,
}

impl<'a> Tuple<'a> {
    fn cell_by_attr(&self, attr: &Attribute) -> Option<Cell> {
        self.cell(attr.pos, attr.kind)
    }

    fn cell(&self, pos: usize, kind: Type) -> Option<Cell> {
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

    fn add_cells(mut self, other: &'a ByteTuple) -> Self {
        self.rest = Some(other);
        self
    }
}

struct Cell<'a> {
    raw: &'a [u8],
    kind: Type,
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
    fn test_join() {
        let attributes_1 = vec![
            Attribute::named(0, "a").with_type(Type::NUMBER),
            Attribute::named(1, "b").with_type(Type::TEXT),
        ];
        let attributes_2 = vec![
            Attribute::named(0, "c").with_type(Type::TEXT),
        ];
        let joined_attributes = vec![
            Attribute::named(0, "a").with_type(Type::NUMBER),
            Attribute::named(1, "b").with_type(Type::TEXT),
            Attribute::named(2, "c").with_type(Type::TEXT),
        ];
        
        let object_1 = vec![
            tuple(&["1", "foo"]),
            tuple(&["2", "bar"]),
        ];
        let joiner = tuple(&["fizz"]);

        let result: Vec<Tuple> = TupleSet::from_object(&object_1).filter_map(|tuple| Some(tuple.add_cells(&joiner))).collect();
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
