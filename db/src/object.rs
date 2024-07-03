use std::fmt;
use std::iter::zip;
use std::{collections::{HashSet, HashMap}, cell::Ref};

use crate::bytes::write_as_bytes;
use crate::schema::AttributeRef;
use crate::tuple::PositionalAttribute;
use crate::{tuple::Tuple, schema::Relation, Type};

type ByteCell = Vec<u8>;
type ByteTuple = Vec<u8>;

#[derive(Default)]
pub(crate) struct IndexedObject {
    id: usize,
    name: Box<str>,
    pub(crate) tuples: Vec<ByteTuple>,
    pub(crate) attrs: Vec<Attribute>,
    index: Index,
    hash: HashMap<ByteCell, usize>,
    removed_ids: HashSet<usize>,
}

impl IndexedObject {
    pub(crate) fn from_table(table: &Relation) -> Self {
        let key = table.indexed_column().map(Attribute::from);

        Self {
            id: table.id,
            name: Box::from(table.name()),
            tuples: Vec::new(),
            attrs: table.attributes().map(Attribute::from).collect(),
            index: key.map(Index::Attr).unwrap_or(Index::Uniq),
            hash: Default::default(),
            removed_ids: Default::default(),
        }
    }

    pub(crate) fn id(&self) -> usize {
        self.id
    }

    pub(crate) fn name(&self) -> &str {
        &self.name
    }

    pub(crate) fn add_tuple(&mut self, tuple: &Tuple) -> bool {
        match &self.index {
            Index::Attr(key_pos) => {
                let key = tuple.element(key_pos);
                if let Some(id) = self.hash.get(key.unwrap().bytes()) {
                    let _ = std::mem::replace(&mut self.tuples[*id], tuple.raw_bytes().to_vec());
                    false
                } else {
                    self.hash.insert(tuple.element(key_pos).expect("Already validated").bytes().to_vec(), self.tuples.len());
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
        self.tuples.get(id).map(|bytes| Tuple::with_object(bytes, self))
    }

    pub(crate) fn iter<'b>(&'b self) -> Box<dyn Iterator<Item = Tuple<'b>> + 'b> {
        Box::new(
            self.tuples.iter().enumerate()
                .filter(|(id, _)| !self.removed_ids.contains(id))
                .map(move |(_, bytes)| Tuple::with_object(bytes, self))
        )
    }

    pub(crate) fn remove_tuples(&mut self, ids: &[usize]) {
        self.removed_ids.extend(ids);
    }

    pub(crate) fn recover(snapshot: TempObject, table: &Relation) -> Self {
        if let Some(key_col) = table.indexed_column() {
            let key = Attribute::from(key_col);
            let index = Index::Attr(key);

            let mut obj = Self {
                id: table.id,
                name: Box::from(table.name()),
                tuples: snapshot.values,
                attrs: table.attributes().map(Attribute::from).collect(),
                index,
                hash: Default::default(),
                removed_ids: HashSet::new(),
            };

            obj.reindex();
            obj
        } else {
            Self {
                id: table.id,
                name: Box::from(table.name()),
                tuples: snapshot.values,
                attrs: table.attributes().map(Attribute::from).collect(),
                index: Default::default(),
                hash: Default::default(),
                removed_ids: HashSet::new(),
            }
        }
    }

    fn reindex(&mut self) {
        if let Index::Attr(key_pos) = &self.index {
            self.hash.clear();
            for (id, raw_tuple) in self.tuples.iter().enumerate() {
                let tuple = Tuple::from_bytes(raw_tuple, &self.attrs);
                self.hash.insert(tuple.element(key_pos).unwrap().bytes().to_vec(), id);
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

    pub(crate) fn attributes(&self) -> impl Iterator<Item = &Attribute> {
        self.attrs.iter()
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
    Attr(Attribute),
}

#[derive(Debug)]
pub struct TempObject {
    values: Vec<Vec<u8>>,
    attrs: Vec<Attribute>,
}

impl TempObject {
    pub fn from_attrs(attrs: &[impl NamedAttribute]) -> Self {
        let attrs = attrs.iter().enumerate().map(|(pos, a)| Attribute {
            pos,
            name: Box::from(a.name()),
            kind: a.kind(),
            reference: AttributeRef::temporary(pos),
        }).collect();

        Self{ values: vec![], attrs }
    }

    pub fn from_relation(rel: &Relation) -> Self {
        let attrs = rel.attributes().map(Attribute::from).collect();
        Self { values: vec![], attrs }
    }

    pub(crate) fn from_object(obj: &IndexedObject) -> Self {
        Self { values: vec![], attrs: obj.attrs.clone() }
    }

    pub(crate) fn build_tuple(self) -> TupleBuilder {
        let raw = (0..self.attrs.len()).map(|_| Vec::new()).collect();
        TupleBuilder {
            obj: self,
            raw,
        }
    }

    pub fn push(&mut self, raw_tuple: &[u8]) {
        self.values.push(raw_tuple.to_vec());
    }

    pub(crate) fn push_str(&mut self, vals: &[impl AsRef<str>]) {
        let tuple = zip(vals.iter(), self.attrs.iter()).fold(Vec::new(), |mut acc, (val, attr)| {
            write_as_bytes(attr.kind(), val.as_ref(), &mut acc).expect("Cannot fail writing to vec");
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

    #[cfg(test)]
    pub(crate) fn attributes(&self) -> impl Iterator<Item = &Attribute> {
        self.attrs.iter()
    }
}

pub(crate) struct TupleBuilder {
    obj: TempObject,
    raw: Vec<Vec<u8>>,
}

impl TupleBuilder {
    pub(crate) fn add(&mut self, attr_ref: &AttributeRef, val: &str) {
        let attr = self.obj.attrs.iter().find(|a| *a == attr_ref).unwrap();
        let mut container = &mut self.raw[attr.pos];
        write_as_bytes(attr.kind(), val, &mut container).expect("Cannot fail with vec");
    }

    pub(crate) fn build(mut self) -> TempObject {
        self.obj.values.push(self.raw.iter().flatten().copied().collect());
        self.obj
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
            name
        }        
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Attribute {
    // TODO: position should be internal to the object (but not schema
    // because the element ordering should be private for the object)
    pub pos: usize,
    pub name: Box<str>,
    pub kind: Type,
    pub reference: AttributeRef,
}

impl PositionalAttribute for Attribute {
    fn pos(&self) -> usize {
        self.pos
    }

    fn object_id(&self) -> Option<usize> {
        self.reference.rel_id
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

impl PartialOrd for Attribute {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        self.pos.partial_cmp(&other.pos)
    }
}

impl Ord for Attribute {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.pos.cmp(&other.pos)
    }
}

impl PartialEq<str> for Attribute {
    fn eq(&self, s: &str) -> bool {
        self.name.as_ref() == s
    }
}

impl PartialEq<str> for &Attribute {
    fn eq(&self, s: &str) -> bool {
        self.name.as_ref() == s
    }
}

impl PartialEq<AttributeRef> for Attribute {
    fn eq(&self, a: &AttributeRef) -> bool {
        &self.reference == a
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

    pub fn raw_tuples(&'a self) -> impl Iterator<Item = &[u8]> {
        self.object.tuples.iter().map(|v| v.as_slice())
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
