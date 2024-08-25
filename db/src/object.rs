use std::cell::RefCell;
use std::{fmt, io};
use std::hash::{DefaultHasher, Hash as _, Hasher};
use std::iter::zip;
use std::rc::Rc;
use std::collections::HashMap;

use crate::bytes::write_as_bytes;
use crate::event::EventHandler;
use crate::heap::Heap;
use crate::schema::AttributeRef;
use crate::tuple::{element_at_pos, PositionalAttribute};
use crate::{tuple::Tuple, schema::Relation, Type};
use crate::page::TupleId;

pub type ObjectId = u32;

#[derive(Default)]
pub(crate) struct IndexedObject {
    id: ObjectId,
    pages: Heap,
    pub(crate) attrs: Vec<Attribute>,
    index: Index,
    hash: HashMap<u64, Vec<TupleId>>,
    handler: Rc<RefCell<EventHandler>>,
}

impl IndexedObject {
    pub(crate) fn from_table(table: &Relation) -> Self {
        let key = table.indexed_column().map(Attribute::from);

        Self {
            id: table.id,
            pages: Heap::default().with_object_id(table.id),
            attrs: table.attributes().map(Attribute::from).collect(),
            index: key.map(Index::Attr).unwrap_or(Index::Uniq),
            ..Default::default()
        }
    }

    pub(crate) fn with_handler(mut self, handler: Rc<RefCell<EventHandler>>) -> Self {
        self.pages = self.pages.with_handler(Rc::clone(&handler));
        self.handler = handler;
        self
    }

    pub(crate) fn id(&self) -> ObjectId {
        self.id
    }

    pub(crate) fn add_tuple(&mut self, tuple: &[u8]) {
        let added = match &self.index {
            Index::Attr(key_attr) => {
                let key = element_at_pos(tuple, &self.attrs, key_attr.pos());
                if let Some(id) = self.find_in_index(key) {
                    self.pages.delete(id);
                    self.remove_from_index(key, id);

                    let new_id = self.pages.push(tuple);
                    self.add_to_index(tuple, new_id);
                    false
                } else {
                    let id = self.pages.push(tuple);
                    self.add_to_index(key, id);
                    true
                }
            },
            Index::Uniq => {
                if self.find_in_index(tuple).is_some() {
                    false
                } else {
                    let id = self.pages.push(tuple);
                    self.add_to_index(tuple, id);
                    true
                }
                
            }
        };
 
       if added {
            self.handler.borrow().emit_add_tuple(self.id, tuple);
        }
    }

    pub(crate) fn find_in_index(&self, bytes: &[u8]) -> Option<TupleId> {
        match &self.index {
            Index::Attr(attr) => {
                let matched_ids = self.hash.get(&hash(bytes))?;
                matched_ids.iter().find(|id| {
                    let tuple = self.get(**id);
                    let elem = tuple.element(attr).unwrap();
                    elem.bytes() == bytes
                }).copied()
            }
            Index::Uniq => {
                let matched_ids = self.hash.get(&hash(bytes))?;
                matched_ids.iter().find(|id| {
                    let tuple = self.get(**id);
                    tuple.raw_bytes() == bytes
                }).copied()
            }
        }
    }

    fn add_to_index(&mut self, bytes: &[u8], id: TupleId) {
        self.hash.entry(hash(bytes))
            .and_modify(|ids| { ids.push(id); })
            .or_insert_with(|| vec![id]);
    }

    fn remove_from_index(&mut self, bytes: &[u8], id: TupleId) {
        self.hash.entry(hash(bytes))
            .and_modify(|v| v.retain(|x| id != *x));
    }

    pub(crate) fn get(&self, id: TupleId) -> Tuple {
        let tuple = self.pages.tuple_by_id(id);
        let contents = tuple.contents();
        Tuple::with_object(contents, self).with_id(id)
    }

    pub(crate) fn iter<'b>(&'b self) -> Box<dyn Iterator<Item = Tuple<'b>> + 'b> {
        Box::new(self.pages.tuples()
                 .map(|tuple| {
                     let id = tuple.id();
                     Tuple::with_object(tuple.contents(), self).with_id(id)
                 })
        )
    }

    pub(crate) fn remove_tuples(&mut self, ids: &[TupleId]) {
        let handler = self.handler.borrow();
        for id in ids {
            handler.emit_delete_tuple(id.into());
            self.pages.delete(*id);
        }
    }

    pub(crate) fn read(&mut self, r: impl io::Read) -> io::Result<()> {
        self.pages.read(r)?;
        self.reindex();
        Ok(())
    }

    fn reindex(&mut self) {
        match &self.index {
            Index::Attr(key) => {
                self.hash.clear();
                for tuple in self.pages.tuples() {
                    let id = tuple.id();
                    let tuple = Tuple::from_bytes(tuple.contents(), &self.attrs);
                    let hash = hash(tuple.element(key).unwrap().bytes());
                    self.hash.entry(hash)
                        .and_modify(|ids| { ids.push(id); })
                        .or_insert_with(|| vec![id]);
                }
            },
            Index::Uniq => {
                self.hash.clear();
                for tuple in self.pages.tuples() {
                    let id = tuple.id();
                    let tuple = Tuple::from_bytes(tuple.contents(), &self.attrs);
                    let hash = hash(tuple.raw_bytes());
                    self.hash.entry(hash)
                        .and_modify(|ids| { ids.push(id); })
                        .or_insert_with(|| vec![id]);
                }
            }
        }
    }

    #[allow(dead_code)]
    pub(crate) fn vaccum(&mut self) {
        let old_heap = std::mem::take(&mut self.pages);
        for tuple in old_heap.tuples() {
            self.pages.push(&tuple.contents());
        }

        self.reindex();
    }

    pub(crate) fn attributes(&self) -> impl Iterator<Item = &Attribute> {
        self.attrs.iter()
    }
}

fn hash(bytes: &[u8]) -> u64 {
    let mut hasher = DefaultHasher::new();
    bytes.hash(&mut hasher);
    hasher.finish()
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
        let attrs = attrs.iter().map(|a| Attribute {
            name: Box::from(a.name()),
            kind: a.kind(),
            reference: AttributeRef { attr_id: a.pos(), rel_id: a.object_id() },
        }).collect();

        Self{ values: vec![], attrs }
    }

    pub fn from_relation(rel: &Relation) -> Self {
        let attrs = rel.attributes().map(Attribute::from).collect();
        Self { values: vec![], attrs }
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
}

pub trait NamedAttribute: PositionalAttribute {
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
    pub name: Box<str>,
    pub kind: Type,
    pub reference: AttributeRef,
}

impl PositionalAttribute for Attribute {
    fn pos(&self) -> u32 {
        self.reference.attr_id
    }

    fn object_id(&self) -> Option<u32> {
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
        self.reference.partial_cmp(&other.reference)
    }
}

impl Ord for Attribute {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.reference.cmp(&other.reference)
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
