use std::fmt;
use std::iter::zip;
use std::{collections::{HashSet, HashMap}, marker::PhantomData, cell::Ref};

use crate::tuple::Cell;
use crate::{tuple::Tuple, schema::Relation, Type};

type ByteCell = Vec<u8>;
type ByteTuple = Vec<u8>;

#[derive(Default)]
pub(crate) struct IndexedObject<'a> {
    pub(crate) tuples: Vec<ByteTuple>,
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
                let key = tuple.cell(key_pos);
                if let Some(id) = self.hash.get(key.unwrap().bytes()) {
                    let _ = std::mem::replace(&mut self.tuples[*id], tuple.raw_bytes().to_vec());
                    false
                } else {
                    self.hash.insert(tuple.cell(key_pos).expect("Already validated").bytes().to_vec(), self.tuples.len());
                    self.tuples.push(tuple.raw_bytes().to_vec());
                    true
                }
            },
            Index::Uniq => {
                if self.tuples.iter().any(|x| x == tuple.raw_bytes()) {
                    false
                } else {
                    self.tuples.push(tuple.raw_bytes().to_vec());
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

    pub(crate) fn recover<'b>(snapshot: TempObject, table: &Relation) -> Self {
        if let Some(key_col) = table.indexed_column() {
            let key = key_col.pos();
            let index = Index::Attr(key);

            let mut obj = Self{
                tuples: snapshot.values,
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
                tuples: snapshot.values,
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
            for (id, raw_tuple) in self.tuples.iter().enumerate() {
                let tuple = Tuple::from_bytes(raw_tuple, &self.attrs);
                self.hash.insert(tuple.cell(key_pos).unwrap().bytes().to_vec(), id);
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
pub struct TempObject {
    values: Vec<Vec<u8>>,
    attrs: Vec<Type>,
}

impl TempObject {
    pub fn with_attrs(attrs: Vec<Type>) -> Self {
        Self{ values: vec![], attrs }
    }

    pub fn push(&mut self, input_tuple: Vec<Vec<u8>>) {
        let mut raw_tuple = Vec::new();
        for (val, kind) in zip(input_tuple.iter(), self.attrs.iter()) {
            if *kind == Type::TEXT {
                raw_tuple.push(val.len() as u8);
            }

            val.iter().for_each(|b| raw_tuple.push(*b));
        }

        self.values.push(raw_tuple);
    }

    pub(crate) fn push_str(&mut self, vals: &[String]) {
        fn cell(val: &str, kind: Type) -> Vec<u8> {
            match kind {
                Type::NUMBER => val.parse::<i32>().map(|n| n.to_be_bytes().to_vec()).unwrap(),
                Type::TEXT => val.as_bytes().to_vec(),
                Type::BOOLEAN => if val == "true" { vec![1] } else if val == "false" { vec![0] } else { panic!() },
                Type::NONE => vec![],
                _ => panic!("unknown type: {}", kind),
            }
        }

        let tuple = zip(vals.iter(), self.attrs.iter()).map(|(val, kind)| cell(val, *kind)).collect();
        self.push(tuple);
    }

    pub(crate) fn iter<'a>(&'a self) -> Box<dyn Iterator<Item = Tuple<'a>> + 'a> {
        Box::new(self.values.iter().map(|bytes| Tuple::from_bytes(bytes, &self.attrs)))
    }

    pub fn is_empty(&self) -> bool {
        self.values.is_empty()
    }

    pub fn len(&self) -> usize {
        self.values.len()
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
