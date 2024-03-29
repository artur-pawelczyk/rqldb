use std::fmt;
use std::iter::zip;
use std::{collections::{HashSet, HashMap}, cell::Ref};

use crate::tuple::PositionalAttribute;
use crate::{tuple::Tuple, schema::Relation, Type};

type ByteCell = Vec<u8>;
type ByteTuple = Vec<u8>;

#[derive(Default)]
pub(crate) struct IndexedObject {
    pub(crate) tuples: Vec<ByteTuple>,
    attrs: Vec<Attribute>,
    index: Index,
    hash: HashMap<ByteCell, usize>,
    removed_ids: HashSet<usize>,
}

impl IndexedObject {
    pub(crate) fn from_table(table: &Relation) -> Self {
        let key = table.indexed_column().map(|col| col.pos());

        Self{
            tuples: Vec::new(),
            attrs: table.attributes().enumerate().map(|(i, attr)| Attribute { pos: i, name: Box::from(attr.name()), kind: attr.kind() }).collect(),
            index: key.map(Index::Attr).unwrap_or(Index::Uniq),
            hash: Default::default(),
            removed_ids: Default::default(),
        }
    }

    pub(crate) fn add_tuple(&mut self, tuple: &Tuple) -> bool {
        match self.index {
            Index::Attr(key_pos) => {
                let key = tuple.element(&key_pos);
                if let Some(id) = self.hash.get(key.unwrap().bytes()) {
                    let _ = std::mem::replace(&mut self.tuples[*id], tuple.raw_bytes().to_vec());
                    false
                } else {
                    self.hash.insert(tuple.element(&key_pos).expect("Already validated").bytes().to_vec(), self.tuples.len());
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
        Box::new(
            self.tuples.iter().enumerate()
                .filter(|(id, _)| !self.removed_ids.contains(id))
                .map(|(_, bytes)| Tuple::from_bytes(bytes, &self.attrs))
        )
    }

    pub(crate) fn remove_tuples(&mut self, ids: &[usize]) {
        self.removed_ids.extend(ids);
    }

    pub(crate) fn recover(snapshot: TempObject, table: &Relation) -> Self {
        if let Some(key_col) = table.indexed_column() {
            let key = key_col.pos();
            let index = Index::Attr(key);

            let mut obj = Self{
                tuples: snapshot.values,
                attrs: table.attributes().enumerate().map(|(i, attr)| Attribute { pos: i, name: Box::from(attr.name()), kind: attr.kind() }).collect(),
                index,
                hash: Default::default(),
                removed_ids: HashSet::new(),
            };

            obj.reindex();
            obj
        } else {
            Self{
                tuples: snapshot.values,
                attrs: table.attributes().enumerate().map(|(i, attr)| Attribute { pos: i, name: Box::from(attr.name()), kind: attr.kind() }).collect(),
                index: Default::default(),
                hash: Default::default(),
                removed_ids: HashSet::new(),
            }
        }
    }

    fn reindex(&mut self) {
        if let Index::Attr(key_pos) = self.index {
            self.hash.clear();
            for (id, raw_tuple) in self.tuples.iter().enumerate() {
                let tuple = Tuple::from_bytes(raw_tuple, &self.attrs);
                self.hash.insert(tuple.element(&key_pos).unwrap().bytes().to_vec(), id);
            }
        }
    }

    pub(crate) fn vaccum(&mut self) {
        let old = std::mem::take(&mut self.tuples);
        self.tuples = old.into_iter().enumerate()
            .filter(|(i, _)| !self.removed_ids.contains(i))
            .map(|(_, tuple)| tuple)
            .collect();

        self.removed_ids.clear();
    }
}

impl fmt::Debug for IndexedObject {
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
    attrs: Vec<Attribute>,
}

impl TempObject {
    pub fn with_attrs(attrs: &[impl NamedAttribute]) -> Self {
        let attrs = attrs.iter().enumerate().map(|(pos, a)| Attribute { pos, name: Box::from(a.name()), kind: a.kind() }).collect();
        Self{ values: vec![], attrs }
    }

    pub fn from_relation(rel: &Relation) -> Self {
        let attrs = rel.attributes().enumerate()
            .map(|(i, attr)| Attribute { pos: i, name: Box::from(attr.name()), kind: attr.kind() })
            .collect();

        Self { values: vec![], attrs }
    }

    pub fn push(&mut self, input_tuple: Vec<Vec<u8>>) {
        let mut raw_tuple = Vec::new();
        for (val, attr) in zip(input_tuple.iter(), self.attrs.iter()) {
            if attr.kind() == Type::TEXT {
                raw_tuple.push(val.len() as u8);
            }

            val.iter().for_each(|b| raw_tuple.push(*b));
        }

        self.values.push(raw_tuple);
    }

    pub(crate) fn push_str(&mut self, vals: &[impl AsRef<str>]) {
        fn add_cell(val: &impl AsRef<str>, kind: Type, tuple: &mut Vec<u8>)  {
            let val = val.as_ref();
            match kind {
                Type::NUMBER => val.parse::<i32>().map(|n| n.to_be_bytes()).unwrap().iter().for_each(|b| tuple.push(*b)),
                Type::TEXT => { tuple.push(val.len() as u8); val.as_bytes().iter().for_each(|b| tuple.push(*b)); },
                Type::BOOLEAN => if val == "true" { tuple.push(1) } else if val == "false" { tuple.push(0) } else { panic!() },
                Type::NONE => {},
                _ => panic!("unknown type: {}", kind),
            };
        }

        let tuple = zip(vals.iter(), self.attrs.iter()).fold(Vec::new(), |mut acc, (val, attr)| {
            add_cell(val, attr.kind(), &mut acc);
            acc
        });

        self.values.push(tuple);
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

pub trait NamedAttribute {
    fn name(&self) -> &str;
    fn kind(&self) -> Type;

    fn short_name(&self) -> &str {
        let name = self.name();
        if let Some(i) = name.find('.') {
            &name[i+1..]
        } else {
            &name
        }        
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct Attribute {
    // TODO: position should be internal to the object (but not schema
    // because the element ordering should be private for the object)
    pub pos: usize,
    pub name: Box<str>,
    pub kind: Type,
}

impl PositionalAttribute for Attribute {
    fn pos(&self) -> usize {
        self.pos
    }
}

impl NamedAttribute for Attribute {
    fn name(&self) -> &str {
        &self.name
    }

    fn kind(&self) -> Type {
        self.kind
    }
}

impl Attribute {
    pub fn named(pos: usize, name: &str) -> Self {
        Self {
            pos,
            name: Box::from(name),
            kind: Type::default(),
        }
    }

    pub fn with_type(self, kind: Type) -> Self {
        Self {
            kind,
            ..self
        }
    }
}

impl PartialEq<str> for Attribute {
    fn eq(&self, s: &str) -> bool {
        self.name.as_ref() == s
    }
}

pub struct RawObjectView<'a> {
    pub(crate) object: Ref<'a, IndexedObject>,
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

    pub fn schema(&self) -> &Relation {
        self.rel
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::schema::Schema;

    #[test]
    fn test_delete_tuples() {
        let mut schema = Schema::default();
        schema.create_table("example")
            .column("id", Type::NUMBER)
            .column("name", Type::TEXT)
            .add();
        let relation = schema.find_relation("example").unwrap();

        let mut temp_obj = TempObject::from_relation(relation);
        temp_obj.push_str(&["1", "first"]);
        temp_obj.push_str(&["2", "second"]);

        let mut obj = IndexedObject::recover(temp_obj, relation);
        assert_eq!(obj.iter().count(), 2);

        obj.remove_tuples(&[0]);
        assert_eq!(obj.iter().count(), 1);
    }

    #[test]
    fn test_vaccum() {
        let mut schema = Schema::default();
        schema.create_table("example")
            .column("id", Type::NUMBER)
            .column("name", Type::TEXT)
            .add();
        let relation = schema.find_relation("example").unwrap();

        let mut temp_obj = TempObject::from_relation(relation);
        temp_obj.push_str(&["1", "first"]);
        temp_obj.push_str(&["2", "second"]);

        let mut obj = IndexedObject::recover(temp_obj, relation);
        obj.remove_tuples(&[0]);

        assert_eq!(obj.tuples.len(), 2);

        obj.vaccum();

        assert_eq!(obj.tuples.len(), 1);
        assert_eq!(obj.iter().count(), 1);
    }
}
