use std::cmp::max;
use std::collections::hash_map::DefaultHasher;
use std::hash::Hasher;

type ByteCell = Vec<u8>;
type ByteTuple = Vec<ByteCell>;

pub(crate) struct Index {
    buckets: Vec<Node>,
    len: usize,
    cell_id: Option<usize>,
}

pub(crate) struct IndexInsertion<'a> {
    tuples: &'a [ByteTuple],
    index: &'a mut Index,
}

impl Index {
    pub(crate) fn new(tuples: &[ByteTuple]) -> Self {
        let mut instance = Self{
            buckets: (0..4).map(|_| Node::End).collect(),
            len: 0,
            cell_id: None
        };

        for tuple in tuples {
            instance.index(tuple, |_| None);
        }

        instance
    }

    pub(crate) fn single_cell(tuples: &[ByteTuple], pos: usize) -> Self {
        let mut instance = Self{
            buckets: (0..16).map(|_| Node::End).collect(),
            len: 0,
            cell_id: Some(pos),
        };

        for tuple in tuples {
            instance.index(tuple, |_| None);
        }

        instance
    }

    fn index<'a, F>(&mut self, byte_tuple: &'a ByteTuple, f: F) -> Op
    where F: Fn(usize) -> Option<&'a ByteTuple>
    {
        let hash = self.hash(byte_tuple);
        if let Some(same_hash_pos) = self.find_bucket(hash) {
            let same_hash_node = &self.buckets[same_hash_pos];
            if let Some(matched) = self.find_in_node(&same_hash_node, byte_tuple, f) {
                Op::Replace(matched)
            } else {
                let id = self.len;
                self.insert_into_bucket(same_hash_pos, id);
                self.len += 1;
                Op::Insert(id)
            }
        } else {
            let id = self.len;
            let pos = hash as usize % self.buckets.len();
            let _ = std::mem::replace(&mut self.buckets[pos], Node::rel(id));
            self.len += 1;
            Op::Insert(id)
        }
    }

    fn find_bucket(&self, hash: u64) -> Option<usize> {
        let pos = hash as usize % self.buckets.len();
        if self.buckets[pos].is_empty() {
            None
        } else {
            Some(pos)
        }
    }

    fn hash(&self, byte_tuple: &ByteTuple) -> u64 {
        let mut hasher = DefaultHasher::new();
        match self.cell_id {
            Some(id) => byte_tuple.get(id).iter().for_each(|x| hasher.write(x)),
            None => byte_tuple.iter().for_each(|x| hasher.write(x)),
        }
        hasher.finish()
    }

    fn find_in_node<'a, F>(&self, node: &Node, byte_tuple: &ByteTuple, f: F) -> Option<usize>
    where F: Fn(usize) -> Option<&'a ByteTuple>
    {
        match self.cell_id {
            Some(cell_id) => node.iter().find(|i| byte_tuple.get(cell_id) == f(*i).and_then(|x| x.get(cell_id))),
            None => node.iter().find(|i| Some(byte_tuple) == f(*i)),
        }
    }

    fn insert_into_bucket(&mut self, bucket_id: usize, entry: usize) {
        let node = std::mem::take(&mut self.buckets[bucket_id]);
        self.buckets[bucket_id] = node.add(entry);
    }

    pub(crate) fn on<'a>(&'a mut self, tuples: &'a [ByteTuple]) -> IndexInsertion<'a> {
        IndexInsertion{ tuples, index: self }
    }

    pub fn print_statistics(&self) {
        let size = self.buckets.len();
        let avg = self.len / size;
        let max = self.buckets.iter().map(|x| x.len()).fold(0usize, |acc, x| max(acc, x));

        println!("bucket size: {}; avg bucket size: {}; longest: {}", size, avg, max);
    }
}

impl<'a> IndexInsertion<'a> {
    pub(crate) fn insert(self, new_tuple: &ByteTuple) -> Op {
        self.index.index(new_tuple, |id| self.tuples.get(id))
    }
}

#[derive(Debug)]
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

    fn iter(&self) -> NodeIter {
        NodeIter{ node: self }
    }

    fn is_empty(&self) -> bool {
        match self {
            Self::End => true,
            Self::Ref(_, _) => false,
        }
    }

    fn len(&self) -> usize {
        match self {
            Node::Ref(_, next) => 1 + next.len(),
            Node::End => 0,
        }
    }
}

impl Default for Node {
    fn default() -> Self {
        Node::End
    }
}

struct NodeIter<'a> {
    node: &'a Node,
}

impl<'a> Iterator for NodeIter<'a> {
    type Item = usize;

    fn next(&mut self) -> Option<usize> {
        match self.node {
            Node::Ref(id, next) => {
                self.node = next;
                return Some(*id);
            },
            Node::End => None
        }
    }
}

pub(crate) enum Op {
    Insert(usize),
    Replace(usize),
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_index_whole_tuple() {
        let tuples = vec![
            byte_tuple(&["1", "one"]),
            byte_tuple(&["2", "two"]),
        ];

        let mut index = Index::new(&tuples);
        assert_eq!(index.len, 2);

        let new_tuple = byte_tuple(&["2", "other two"]);
        assert!(matches!(index.on(&tuples).insert(&new_tuple), Op::Insert(2)));
        assert_eq!(index.len, 3);

        let duplicate = byte_tuple(&["2", "two"]);
        assert!(matches!(index.on(&tuples).insert(&duplicate), Op::Replace(1)));
        assert_eq!(index.len, 3);
    }

    #[test]
    fn test_index_first_cell() {
        let tuples = vec![
            byte_tuple(&["1", "content"]),
            byte_tuple(&["2", "content"]),
        ];

        let mut index = Index::single_cell(&tuples, 0);

        let replacement = byte_tuple(&["1", "new content"]);
        let op = index.on(&tuples).insert(&replacement);
        assert!(matches!(op, Op::Replace(0)));
    }

    fn byte_tuple(cells: &[&str]) -> ByteTuple {
        cells.iter().map(|s| {
            if let Result::Ok(num) = s.parse::<i32>() {
                Vec::from(num.to_be_bytes())
            } else {
                Vec::from(s.as_bytes())
            }
        }).collect()
    }
}
