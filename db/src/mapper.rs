use std::ops::Range;

use crate::{database::SharedObject, object::{Attribute, NamedAttribute as _}, schema::AttributeRef, tuple::{PositionalAttribute, Tuple}};

#[derive(Default)]
pub(crate) struct OutTuple<'a> {
    attrs: &'a [Attribute],
    raw: Vec<u8>,
}

impl<'a> OutTuple<'a> {
    pub(crate) fn element(&self, attr: &impl PositionalAttribute) -> Option<&[u8]> {
        self.element_range(attr)
            .map(|r| &self.raw[r])
    }

    pub(crate) fn update(&mut self, attr: &impl PositionalAttribute, value: &[u8]) {
        if let Some(range) = self.element_range(attr) {
            self.raw.splice(range, value.iter().copied());
        }
    }

    fn element_range(&self, attr: &impl PositionalAttribute) -> Option<Range<usize>> {
        let mut offset = 0usize;
        for a in self.attrs {
            let size = a.kind().size(&self.raw[offset..]);
            if a.pos() == attr.pos() && a.object_id() == attr.object_id() {
                return Some(offset..offset+size);
            }

            offset += size;
        }

        None
    }

    pub(crate) fn into_raw(self) -> Vec<u8> {
        self.raw
    }
}

impl<'a> From<Tuple<'a>> for OutTuple<'a> {
    fn from(tuple: Tuple<'a>) -> Self {
        Self {
            attrs: tuple.attrs,
            raw: tuple.raw.to_vec(),
        }
    }
}

pub(crate) trait Mapper<'a> {
    fn apply(&'a self, output: OutTuple) -> Option<OutTuple<'a>>;
    fn attributes_after(&self) -> Vec<Attribute>;
}

#[cfg(test)]
pub(crate) struct AppendMapper<'a> {
    pub(crate) raw: &'a [u8],
    pub(crate) attributes_after: Box<[Attribute]>,
}

#[cfg(test)]
impl<'a> Mapper<'a> for AppendMapper<'a> {
    fn apply(&'a self, output: OutTuple) -> Option<OutTuple<'a>> {
        let mut bytes = output.into_raw();
        bytes.extend(self.raw);
        Some(OutTuple {
            attrs: &self.attributes_after,
            raw: bytes
        })
    }

    fn attributes_after(&self) -> Vec<Attribute> {
        self.attributes_after.iter().cloned().collect()
    }
}

pub(crate) struct JoinMapper {
    pub(crate) joiner: SharedObject,
    pub(crate) joinee_key: AttributeRef,
    pub(crate) joiner_key: AttributeRef,
    pub(crate) attributes_after: Box<[Attribute]>,
}

impl<'a> Mapper<'a> for JoinMapper {
    fn apply(&'a self, output: OutTuple) -> Option<OutTuple<'a>> {
        let key = output.element(&self.joinee_key)?;
        if let Some(join_tuple) = self.joiner.borrow().iter()
            .find(|join_tuple| join_tuple.element(&self.joiner_key).map(|e| e.bytes() == key).unwrap_or(false)) {
                let mut raw = output.into_raw();
                raw.extend(join_tuple.raw_bytes());
                return Some(OutTuple {
                    attrs: &self.attributes_after,
                    raw,
                })
            } else {
                None
            }
    }

    fn attributes_after(&self) -> Vec<Attribute> {
        self.attributes_after.iter().cloned().collect()
    }
}

pub(crate) struct NoopMapper(pub Box<[Attribute]>);
impl<'a> Mapper<'a> for NoopMapper {
    fn apply(&'a self, output: OutTuple) -> Option<OutTuple<'a>> {
        Some(OutTuple {
            attrs: &self.0,
            raw: output.into_raw(),
        })
    }

    fn attributes_after(&self) -> Vec<Attribute> {
        self.0.iter().cloned().collect()
    }
}

pub(crate) struct SetMapper {
    pub(crate) attr: Attribute,
    pub(crate) value: Box<[u8]>,
    pub(crate) attributes_after: Box<[Attribute]>,
}

impl<'a> Mapper<'a> for SetMapper {
    fn apply(&'a self, mut output: OutTuple) -> Option<OutTuple<'a>> {
        output.update(&self.attr, &self.value);
        Some(OutTuple {
            attrs: &self.attributes_after,
            raw: output.into_raw(),
        })
    }

    fn attributes_after(&self) -> Vec<Attribute> {
        self.attributes_after.iter().cloned().collect()
    }
}


#[cfg(test)]
mod tests {
    use std::{collections::HashSet, error::Error};

    use crate::{bytes::into_bytes, hash_set, object::NamedAttribute as _, test::fixture::{Dataset, DocType, Document}};

    use super::*;

    #[test]
    fn append_tuple() -> Result<(), Box<dyn Error>> {
        let db = Dataset::default()
            .add(Document::size(1))
            .db();
        let obj = db.object("document").unwrap().borrow();
        let attrs = obj.attributes().cloned().collect();
        let obj_tuple = obj.iter().next().unwrap();
        let mapper = AppendMapper {
            raw: obj_tuple.raw_bytes(),
            attributes_after: attrs,
        };

        let mut out = OutTuple::default();
        out = mapper.apply(out).unwrap();

        assert_eq!(
            out.attrs.iter().map(|a| a.name()).collect::<HashSet<_>>(),
            hash_set!["document.id", "document.content"]
        );

        let expected_id = into_bytes(crate::Type::NUMBER, "1")?;
        assert_eq!(
            out.element(&db.attribute("document.id").unwrap()),
            Some(expected_id.as_slice())
        );

        Ok(())
    }

    #[test]
    fn join_mapper() {
        let db = Dataset::default()
            .add(Document::size(1))
            .add(DocType::size(1))
            .db();

        let doc_attributes: Vec<_> = db.object("document").unwrap().borrow().attributes().cloned().collect();
        let typ_attributes: Vec<_> = db.object("type").unwrap().borrow().attributes().cloned().collect();
        let attributes_after: Vec<_> = doc_attributes.iter().chain(typ_attributes.iter()).cloned().collect();

        let obj = db.object("document").unwrap().borrow();
        let mut output = Some(OutTuple::from(obj.iter().next().unwrap()));

        let join_mapper = JoinMapper {
            joiner: db.object("type").unwrap().clone(),
            joinee_key: db.attribute("document.type_id").unwrap(),
            joiner_key: db.attribute("type.id").unwrap(),
            attributes_after: attributes_after.into(),
        };

        output = join_mapper.apply(output.unwrap());

        assert!(!output.unwrap().raw.is_empty());
    }
}
