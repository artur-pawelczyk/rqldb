use std::fmt;
use std::{collections::{HashSet, HashMap}, marker::PhantomData, cell::Ref};

use crate::{tuple::Tuple, schema::Relation, Type};

type ByteCell = Vec<u8>;
type ByteTuple = Vec<ByteCell>;

#[derive(Default)]
pub(crate) struct IndexedObject<'a> {
    tuples: Vec<ByteTuple>,
    attrs: Vec<Type>,
    index: Index,
    hash: HashMap<ByteCell, usize>,
    removed_ids: HashSet<usize>,
    marker: PhantomData<&'a ()>,
}

impl<'a> IndexedObject<'a> {
    pub(crate) fn from_table(table: &Relation) -> Self {
        let key = table.indexed_column().map(|col| col.pos());

        Self{
            tuples: Vec::new(),
            attrs: table.attributes().iter().map(|(_, kind)| *kind).collect(),
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
                if self.tuples.iter().any(|x| x == tuple.as_bytes()) {
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

    pub(crate) fn get(&self, id: usize) -> Option<Tuple> {
        self.tuples.get(id).map(|bytes| Tuple::from_bytes(bytes, &self.attrs))
    }

    pub(crate) fn iter<'b>(&'b self) -> Box<dyn Iterator<Item = Tuple<'b>> + 'b> {
        Box::new(self.tuples.iter().map(|bytes| Tuple::from_bytes(bytes, &self.attrs)))
    }

    pub(crate) fn remove_tuples(&mut self, ids: &[usize]) {
        self.removed_ids.extend(ids);
    }

    pub(crate) fn is_removed(&self, id: usize) -> bool {
        self.removed_ids.contains(&id)
    }

    pub(crate) fn recover<'b, S: Iterator<Item = Tuple<'b>>>(snapshot: S, table: &Relation) -> Self {
        if let Some(key_col) = table.indexed_column() {
            let key = key_col.pos();
            let index = Index::Attr(key);
            let mut tuples = Vec::with_capacity(snapshot.size_hint().0);
            for tuple in snapshot {
                tuples.push(tuple.as_bytes().clone());
            }

            let mut obj = Self{
                tuples,
                attrs: table.attributes().iter().map(|(_, kind)| *kind).collect(),
                index,
                hash: Default::default(),
                removed_ids: HashSet::new(),
                marker: PhantomData,
            };

            obj.reindex();
            obj
        } else {
            Self{
                tuples: snapshot.map(|t| t.as_bytes().clone()).collect(),
                attrs: table.attributes().iter().map(|(_, kind)| *kind).collect(),
                index: Default::default(),
                hash: Default::default(),
                removed_ids: HashSet::new(),
                marker: PhantomData,
            }
        }
    }

    fn reindex(&mut self) {
        if let Index::Attr(key_pos) = self.index {
            self.hash.clear();
            for (id, tuple) in self.tuples.iter().enumerate() {
                self.hash.insert(tuple[key_pos].to_vec(), id);
            }
        }
    }
}

impl<'a> fmt::Debug for IndexedObject<'a> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "IndexedObject")
    }
}

#[derive(Default)]
enum Index {
    #[default]
    Uniq,
    Attr(usize),
}

#[derive(Debug)]
pub(crate) struct TempObject {
    values: Vec<Vec<u8>>,
    attrs: Vec<Type>,
}

impl TempObject {
    pub(crate) fn new() -> Self {
        Self{ values: vec![], attrs: vec![] }
    }

    pub(crate) fn push(&mut self, val: &str, kind: Type) {
        let cell = match kind {
            Type::NUMBER => val.parse::<i32>().map(|n| n.to_be_bytes().to_vec()).unwrap(),
            Type::TEXT => val.as_bytes().to_vec(),
            Type::BOOLEAN => if val == "true" { vec![1] } else if val == "false" { vec![0] } else { panic!() },
            Type::NONE => vec![],
            _ => panic!("unknown type: {}", kind),
        };

        self.values.push(cell);
        self.attrs.push(kind);
    }

    pub(crate) fn iter<'a>(&'a self) -> Box<dyn Iterator<Item = Tuple<'a>> + 'a> {
        Box::new(std::iter::once(Tuple::from_bytes(&self.values, &self.attrs)))
    }
}

pub struct RawObjectView<'a> {
    pub(crate) object: Ref<'a, IndexedObject<'a>>,
    pub(crate) rel: &'a Relation,
}

impl<'a> RawObjectView<'a> {
    pub fn count(&self) -> usize {
        self.object.tuples.len()
    }

    pub fn raw_tuples(&'a self) -> Box<dyn Iterator<Item = Tuple<'a>> + 'a> {
        Box::new(self.object.tuples.iter().map(|t| Tuple::from_bytes(t, &self.object.attrs)))
    }

    pub fn name(&self) -> &str {
        self.rel.name()
    }

    pub fn types(&self) -> Vec<Type> {
        self.rel.types()
    }
}
