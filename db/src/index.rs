use std::fmt;
use std::collections::HashMap;

use crate::tuple::Tuple;

type ByteTuple = Vec<Vec<u8>>;

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

    fn find<'a, F>(&self, t: &ByteTuple, f: F) -> Option<usize>
    where F: Fn(usize) -> &'a ByteTuple
    {
        let mut current = self;
        loop {
            if let Self::Ref(i, next) = current {
                if t == f(*i) {
                    return Some(*i);
                }
                current = next;
            } else {
                break;
            }
        }

        None
    }

    fn as_vec(&self) -> Vec<usize> {
        let mut out = vec![];
        let mut current = self;

        loop {
            if let Self::Ref(i, next) = current {
                out.push(*i);
                current = next;
            } else {
                break;
            }
        }

        out
    }
}

impl fmt::Debug for Node {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:?}", self.as_vec())
    }
}

impl TupleIndex {
    pub fn new(tuples: &[ByteTuple]) -> Self {
        let mut index: HashMap<u64, Node> = HashMap::new();

        tuples.iter().enumerate().for_each(|(i, byte_tuple)| {
            let hash = Tuple::from_bytes(byte_tuple).hash();
            if let Some(already_indexed) = index.get(&hash) {
                if already_indexed.as_vec().iter().all(|i| &tuples[*i] != byte_tuple) {
                    let entry = index.remove(&hash).map(|x| x.add(i)).unwrap_or_else(|| Node::rel(i));
                    index.insert(hash, entry);
                }
            } else {
                let entry = index.remove(&hash).map(|x| x.add(i)).unwrap_or_else(|| Node::rel(i));
                index.insert(hash, entry);
            }
        });

        Self{ index }
    }

    fn count(&self) -> usize {
        self.index.values().fold(0usize, |acc, node| acc + node.len())
    }

    fn insert(&mut self, tuple: &Tuple) -> usize {
        let id = self.count();
        let hash = tuple.hash();
        let node = self.index.remove(&hash).unwrap_or_else(|| Node::new());
        self.index.insert(hash, node.add(id));
        id
    }

    pub fn index<'a, F>(&mut self, tuple: &Tuple, f: F) -> Op
    where F: Fn(usize) -> &'a ByteTuple
    {
        let hash = tuple.hash();
        if let Some(already_indexed) = self.index.get(&hash) {
            if let Some(_) = already_indexed.find(tuple.as_bytes(), f) {
                Op::Ignore
            } else {
                Op::Insert(self.insert(tuple))
            }
        } else {
            Op::Insert(self.insert(tuple))
        }
    }
}

pub(crate) enum Op {
    Insert(usize),
    Replace(usize),
    Ignore,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_ignore_duplicates() {
        let tuples = vec![
            byte_tuple(&["1", "aaa"]),
            byte_tuple(&["2", "bbb"]),
            byte_tuple(&["2", "bbb"]),
        ];

        let index = TupleIndex::new(&tuples);
        assert_eq!(index.count(), 2);
    }

    #[test]
    fn test_empty_index() {
        let mut index = TupleIndex::new(&[]);
        let tuple_1 = byte_tuple(&["1", "aaa"]);

        assert!(matches!(index.index(&Tuple::from_bytes(&tuple_1), |_| panic!()), Op::Insert(0)));
    }

    #[test]
    fn test_generate_id() {
        let mut tuples = vec![
            byte_tuple(&["1", "aaa"]),
            byte_tuple(&["2", "bbb"]),
        ];
        let mut index = TupleIndex::new(&tuples);

        let new_tuple = byte_tuple(&["3", "ccc"]);
        let new_entry = index.index(&Tuple::from_bytes(&new_tuple), |i| tuples.get(i).unwrap());
        assert!(matches!(new_entry, Op::Insert(2)));
        tuples.insert(2, new_tuple.clone());

        let inserted_again = index.index(&Tuple::from_bytes(&new_tuple), |i| tuples.get(i).unwrap());
        assert!(matches!(inserted_again, Op::Ignore));
    }

    fn byte_tuple(cells: &[&str]) -> ByteTuple {
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
