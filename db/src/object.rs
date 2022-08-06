use std::{collections::HashSet, marker::PhantomData};

use crate::{tuple::Tuple, schema::Relation, idmap::{IdMapIter, IdMap, HashIdMap, InsertResult, KeyExtractor}};

type ByteCell = Vec<u8>;
type ByteTuple = Vec<ByteCell>;

pub(crate) trait Object {
    fn add_tuple(&mut self, tuple: &Tuple) -> bool;
    fn iter<'a>(&'a self) -> IdMapIter<'a, ByteTuple>;
    fn remove_tuples(&mut self, ids: &[usize]);
    fn recover(snapshot: Vec<ByteTuple>, table: &Relation) -> Self;
}

#[allow(dead_code)]
pub(crate) struct IndexedObject<'a> {
    pub(crate) tuples: Box<dyn IdMap<ByteTuple>>,
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
        let indexed_column = table.indexed_column();
        let tuples: Box<dyn IdMap<ByteTuple>> = if let Some(col) = indexed_column {
            Box::new(HashIdMap::with_key_extractor(TupleKeyExtractor(col.pos())))
        } else {
            Box::new(HashIdMap::new())
        };
        
        Self{
            tuples,
            removed_ids: HashSet::new(),
            key: table.indexed_column().map(|col| col.pos()),
            marker: PhantomData,
        }
    }
}

impl<'a> Object for IndexedObject<'a> {
    fn add_tuple(&mut self, tuple: &Tuple) -> bool {
        match self.tuples.insert(tuple.as_bytes().to_vec()) {
            InsertResult::Added(_) => true,
            InsertResult::Replaced(_) => false,
        }
    }

    fn iter<'b>(&'b self) -> IdMapIter<'b, ByteTuple> {
        self.tuples.iter()
    }

    fn remove_tuples(&mut self, ids: &[usize]) {
        self.removed_ids.extend(ids);
    }

    fn recover(snapshot: Vec<ByteTuple>, table: &Relation) -> Self {
        if let Some(key_col) = table.indexed_column() {
            let key = key_col.pos();

            Self{
                tuples: Box::new(HashIdMap::from_with_key_extractor(snapshot, TupleKeyExtractor(key))),
                removed_ids: HashSet::new(),
                key: Some(key),
                marker: PhantomData,
            }

        } else {
            Self{
                tuples: Box::new(HashIdMap::from(snapshot)),
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
            tuples: Box::new(HashIdMap::new()),
            removed_ids: HashSet::new(),
            key: None,
            marker: PhantomData,
        }
    }
}

struct TupleKeyExtractor(usize);
impl KeyExtractor<ByteTuple, ByteCell> for TupleKeyExtractor {
    fn extract<'a>(&self, tuple: &'a ByteTuple) -> &'a ByteCell {
        &tuple[self.0]
    }
}

