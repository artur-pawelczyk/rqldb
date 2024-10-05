use crate::{database::SharedObject, object::Attribute, schema::AttributeRef, tuple::Tuple};

#[derive(Default)]
pub(crate) struct OutTuple<'a> {
    attrs: &'a [Attribute],
    raw: Vec<u8>,
}

impl<'a> OutTuple<'a> {
    fn into_raw(self) -> Vec<u8> {
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
    fn apply(&'a self, output: OutTuple) -> OutTuple<'a>;
}

pub(crate) struct AppendMapper<'a> {
    raw: &'a [u8],
    attributes_after: Box<[Attribute]>,
}

impl<'a> Mapper<'a> for AppendMapper<'a> {
    fn apply(&'a self, output: OutTuple) -> OutTuple<'a> {
        let mut bytes = output.into_raw();
        bytes.extend(self.raw);
        OutTuple {
            attrs: &self.attributes_after,
            raw: bytes
        }
    }
}

struct JoinMapper {
    joiner: SharedObject,
    joinee_key: AttributeRef,
    joiner_key: AttributeRef,
    attributes_after: Box<[Attribute]>,
}

impl<'a> Mapper<'a> for JoinMapper {
    fn apply(&'a self, output: OutTuple) -> OutTuple<'a> {
        todo!()
    }
}

#[cfg(test)]
mod tests {
    use std::iter;

    use crate::test::fixture::{Dataset, DocType, Document};

    use super::*;

    #[test]
    fn append_tuple() {
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
        out = mapper.apply(out);

        assert!(!out.attrs.is_empty());
        assert!(!out.into_raw().is_empty());
    }

    #[ignore]
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
        let mut output = OutTuple::from(obj.iter().next().unwrap());

        let join_mapper = JoinMapper {
            joiner: db.object("type").unwrap().clone(),
            joinee_key: db.attribute("document.type_id").unwrap(),
            joiner_key: db.attribute("type.id").unwrap(),
            attributes_after: attributes_after.into(),
        };
        
        output = join_mapper.apply(output);

        assert!(output.raw.is_empty());
    }
}
