use std::{collections::HashSet, marker::PhantomData};

use crate::{tuple::Tuple, schema::Relation};

type ByteCell = Vec<u8>;
type ByteTuple = Vec<ByteCell>;

pub(crate) trait Object {
    fn add_tuple(&mut self, tuple: &Tuple) -> bool;
    fn iter<'a>(&'a self) -> Box<dyn Iterator<Item = &'a ByteTuple> + 'a>;
    fn remove_tuples(&mut self, ids: &[usize]);
    fn recover(snapshot: Vec<ByteTuple>, table: &Relation) -> Self;
}

#[allow(dead_code)]
pub(crate) struct IndexedObject<'a> {
    pub(crate) tuples: Vec<ByteTuple>,
    key: Option<usize>,
    pub(crate) removed_ids: HashSet<usize>,
    marker: PhantomData<&'a ()>,
}

impl<'a> IndexedObject<'a> {
    pub(crate) fn temporary(byte_tuple: ByteTuple) -> Self {
        let mut obj = Self::default();
        obj.add_tuple(&Tuple::from_bytes(&byte_tuple));
        obj
    }

    pub(crate) fn from_table(table: &Relation) -> Self {
        Self{
            tuples: Vec::new(),
            removed_ids: HashSet::new(),
            key: table.indexed_column().map(|col| col.pos()),
            marker: PhantomData,
        }
    }
}

impl<'a> Object for IndexedObject<'a> {
    fn add_tuple(&mut self, tuple: &Tuple) -> bool {
        match self.key {
            Some(pos) => {
                let to_find = &tuple.as_bytes()[pos];
                if let Some(id) = self.tuples.iter().position(|x| &x[pos] == to_find) {
                    let _ = std::mem::replace(&mut self.tuples[id], tuple.as_bytes().to_vec());
                    false
                } else {
                    self.tuples.push(tuple.as_bytes().to_vec());
                    true
                }
            },
            None => {
                if let Some(_) = self.tuples.iter().position(|x| x == tuple.as_bytes()) {
                    false
                } else {
                    self.tuples.push(tuple.as_bytes().to_vec());
                    true
                }
                
            }
        }
    }

    fn iter<'b>(&'b self) -> Box<dyn Iterator<Item = &'b ByteTuple> + 'b> {
        Box::new(self.tuples.iter())
    }

    fn remove_tuples(&mut self, ids: &[usize]) {
        self.removed_ids.extend(ids);
    }

    fn recover(snapshot: Vec<ByteTuple>, table: &Relation) -> Self {
        if let Some(key_col) = table.indexed_column() {
            let key = key_col.pos();

            Self{
                tuples: snapshot,
                removed_ids: HashSet::new(),
                key: Some(key),
                marker: PhantomData,
            }

        } else {
            Self{
                tuples: snapshot,
                removed_ids: HashSet::new(),
                key: None,
                marker: PhantomData,
            }            
        }
    }
}

impl<'a> Default for IndexedObject<'a> {
    fn default() -> Self {
        IndexedObject{
            tuples: Vec::new(),
            removed_ids: HashSet::new(),
            key: None,
            marker: PhantomData,
        }
    }
}


