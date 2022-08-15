use std::{collections::{HashSet, HashMap}, marker::PhantomData, cell::Ref};

use crate::{tuple::Tuple, schema::Relation};

type ByteCell = Vec<u8>;
type ByteTuple = Vec<ByteCell>;

#[allow(dead_code)]
#[derive(Default)]
pub(crate) struct IndexedObject<'a> {
    tuples: Vec<ByteTuple>,
    index: Index,
    hash: HashMap<ByteCell, usize>,
    removed_ids: HashSet<usize>,
    marker: PhantomData<&'a ()>,
}

impl<'a> IndexedObject<'a> {
    pub(crate) fn temporary(byte_tuple: ByteTuple) -> Self {
        let mut obj = Self::default();
        obj.add_tuple(&Tuple::from_bytes(&byte_tuple));
        obj
    }

    pub(crate) fn from_table(table: &Relation) -> Self {
        let key = table.indexed_column().map(|col| col.pos());

        Self{
            tuples: Vec::new(),
            index: key.map(Index::Attr).unwrap_or(Index::Uniq),
            hash: Default::default(),
            removed_ids: Default::default(),
            marker: PhantomData,
        }
    }

    pub(crate) fn add_tuple(&mut self, tuple: &Tuple) -> bool {
        match self.index {
            Index::Attr(key_pos) => {
                let key = tuple.as_bytes().get(key_pos);
                if let Some(id) = self.hash.get(key.unwrap()) {
                    let _ = std::mem::replace(&mut self.tuples[*id], tuple.as_bytes().to_vec());
                    false
                } else {
                    self.hash.insert(tuple.as_bytes()[key_pos].clone(), self.tuples.len());
                    self.tuples.push(tuple.as_bytes().to_vec());
                    true
                }
            },
            Index::Uniq => {
                if self.tuples.iter().position(|x| x == tuple.as_bytes()).is_some() {
                    false
                } else {
                    self.tuples.push(tuple.as_bytes().to_vec());
                    true
                }
                
            }
        }
    }

    pub(crate) fn find_in_index(&self, cell: &[u8]) -> Option<usize> {
        match self.index {
            Index::Attr(_) => {
                self.hash.get(cell).copied()
            }
            _ => todo!(),
        }
    }

    pub(crate) fn get(&self, id: usize) -> Option<&ByteTuple> {
        self.tuples.get(id)
    }

    pub(crate) fn iter<'b>(&'b self) -> Box<dyn Iterator<Item = &'b ByteTuple> + 'b> {
        Box::new(self.tuples.iter())
    }

    pub(crate) fn remove_tuples(&mut self, ids: &[usize]) {
        self.removed_ids.extend(ids);
    }

    pub(crate) fn is_removed(&self, id: usize) -> bool {
        self.removed_ids.contains(&id)
    }

    pub(crate) fn recover(snapshot: Vec<ByteTuple>, table: &Relation) -> Self {
        if let Some(key_col) = table.indexed_column() {
            let key = key_col.pos();
            let index = Index::Attr(key);
            let mut hash = HashMap::with_capacity(snapshot.len());
            for (id, tuple) in snapshot.iter().enumerate() {
                hash.insert(tuple[key].clone(), id);
            }

            Self{
                tuples: snapshot,
                index, hash,
                removed_ids: HashSet::new(),
                marker: PhantomData,
            }
        } else {
            Self{
                tuples: snapshot,
                index: Default::default(),
                hash: Default::default(),
                removed_ids: HashSet::new(),
                marker: PhantomData,
            }
        }
    }
}

#[derive(Default)]
enum Index {
    #[default]
    Uniq,
    Attr(usize),
}

pub struct RawObjectView<'a> {
    object: Ref<'a, IndexedObject<'a>>,
}

impl<'a> RawObjectView<'a> {
    pub(crate) fn new(object: Ref<'a, IndexedObject<'a>>) -> Self {
        Self{ object }
    }

    pub fn count(&self) -> usize {
        self.object.tuples.len()
    }

    pub fn raw_tuples(&'a self) -> Box<dyn Iterator<Item = &'a ByteTuple> + 'a> {
        Box::new(self.object.tuples.iter())
    }
}
