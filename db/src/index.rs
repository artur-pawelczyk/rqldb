use std::collections::HashMap;

use crate::tuple::Tuple;

type ByteTuple = Vec<Vec<u8>>;

#[derive(Default)]
pub(crate) struct TupleIndex {
    index: HashMap<u64, Node>,
}

enum Node {
    Ref(usize, Box<Node>),
    End,
}

impl Node {
    fn new() -> Self {
        Node::End
    }

    fn rel(id: usize) -> Self {
        Self::new().add(id)
    }

    fn add(self, id: usize) -> Self {
        Node::Ref(id, Box::new(self))
    }

    fn len(&self) -> usize {
        match self {
            Self::Ref(_, rest) => 1 + rest.len(),
            Self::End => 0,
        }
    }
}

impl TupleIndex {
    fn new(tuples: &[ByteTuple]) -> Self {
        let mut index: HashMap<u64, Node> = HashMap::new();

        tuples.iter().enumerate().for_each(|(i, byte_tuple)| {
            let hash = Tuple::from_bytes(byte_tuple).hash();
            let entry = index.remove(&hash).map(|x| x.add(i)).unwrap_or_else(|| Node::rel(i));
            index.insert(hash, entry);
        });

        Self{ index }
    }

    fn count(&self) -> usize {
        self.index.values().fold(0usize, |acc, node| acc + node.len())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_find() {
        let tuples = vec![
            tuple(&["1", "aaa"]),
            tuple(&["2", "bbb"]),
            tuple(&["2", "bbb"]),
        ];

        let index = TupleIndex::new(&tuples);
        assert_eq!(index.count(), 2);
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
