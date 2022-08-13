use std::{collections::{HashSet, HashMap}, marker::PhantomData};

use crate::{tuple::Tuple, schema::Relation};

type ByteCell = Vec<u8>;
type ByteTuple = Vec<ByteCell>;

pub(crate) trait Object {
    fn add_tuple(&mut self, tuple: &Tuple) -> bool;
    fn iter<'a>(&'a self) -> Box<dyn Iterator<Item = &'a ByteTuple> + 'a>;
    fn remove_tuples(&mut self, ids: &[usize]);
    fn recover(snapshot: Vec<ByteTuple>, table: &Relation) -> Self;
    fn find_in_index(&self, cell: &ByteCell) -> Option<&ByteTuple>;
}

#[allow(dead_code)]
#[derive(Default)]
pub(crate) struct IndexedObject<'a> {
    pub(crate) tuples: Vec<ByteTuple>,
    index: Index,
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
        let key = table.indexed_column().map(|col| col.pos());

        Self{
            tuples: Vec::new(),
            index: key.map(|pos| Index::attr(pos)).unwrap_or(Index::Uniq),
            removed_ids: HashSet::new(),
            marker: PhantomData,
        }
    }
}

impl<'a> Object for IndexedObject<'a> {
    fn add_tuple(&mut self, tuple: &Tuple) -> bool {
        match self.index {
            Index::Attr(_, _) => {
                if let Some(id) = self.index.find(tuple.as_bytes()) {
                    let _ = std::mem::replace(&mut self.tuples[id], tuple.as_bytes().to_vec());
                    false
                } else {
                    self.index.index(tuple.as_bytes(), self.tuples.len());
                    self.tuples.push(tuple.as_bytes().to_vec());
                    true
                }
            },
            Index::Uniq => {
                if let Some(_) = self.tuples.iter().position(|x| x == tuple.as_bytes()) {
                    false
                } else {
                    self.tuples.push(tuple.as_bytes().to_vec());
                    true
                }
                
            }
        }
    }

    fn find_in_index(&self, cell: &ByteCell) -> Option<&ByteTuple> {
        match self.index {
            Index::Attr(pos, _) => {
                let mut fake_tuple: ByteTuple = (0..pos).map(|_| Vec::new()).collect();
                fake_tuple.push(cell.clone());
                self.index.find(&fake_tuple).and_then(|id| self.tuples.get(id))
            }
            _ => todo!(),
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
            let index = Index::from(&snapshot, key);

            Self{
                tuples: snapshot,
                index,
                removed_ids: HashSet::new(),
                marker: PhantomData,
            }
        } else {
            Self{
                tuples: snapshot,
                index: Default::default(),
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
    Attr(usize, HashMap<ByteCell, usize>),
}

impl Index {
    fn attr(pos: usize) -> Self {
        Self::Attr(pos, HashMap::new())
    }

    fn from(tuples: &[ByteTuple], pos: usize) -> Self {
        let mut inner = HashMap::with_capacity(tuples.len());
        for (id, tuple) in tuples.iter().enumerate() {
            inner.insert(tuple[pos].clone(), id);
        }

        Self::Attr(pos, inner)
    }

    fn index(&mut self, tuple: &ByteTuple, id: usize) {
        if let Self::Attr(pos, inner) = self {
            inner.insert(tuple[*pos].clone(), id);
        }
    }

    fn find(&self, tuple: &ByteTuple) -> Option<usize> {
        match self {
            Self::Uniq => None,
            Self::Attr(pos, inner) => inner.get(&tuple[*pos]).map(|x| *x),
        }
    }
}
