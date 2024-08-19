use std::cell::Ref;

use crate::{object::{IndexedObject, TempObject}, page::TupleId, plan::Source, tuple::Tuple};

pub(crate) enum ObjectView<'a> {
    Ref(Ref<'a, IndexedObject>),
    TupleRef(Ref<'a, IndexedObject>, TupleId),
    Val(TempObject),
    Empty,
}

impl<'a> From<&'a Source> for ObjectView<'a> {
    fn from(source: &'a Source) -> Self {
        match source {
            Source::TableScan(obj) => {
                ObjectView::Ref(obj.borrow())
            },
            Source::Tuple(values_map) => {
                let attrs = source.attributes();
                let mut temp_object = TempObject::from_attrs(&attrs);
                let values: Vec<_> = values_map.values().collect();
                temp_object.push_str(&values);
                ObjectView::Val(temp_object)
            },
            Source::IndexScan(obj, val) => {
                if let Some(tuple_id) = obj.borrow().find_in_index(val) {
                    ObjectView::TupleRef(obj.borrow(), tuple_id)
                } else {
                    ObjectView::Empty
                }
            },
            _ => unimplemented!()
        }
    }
}

impl<'a> ObjectView<'a> {
    pub(crate) fn iter(&'a self) -> Box<dyn Iterator<Item = Tuple<'a>> + 'a> {
        match self {
            Self::Ref(o) => o.iter(),
            Self::TupleRef(o, id) => Box::new(std::iter::once_with(|| o.get(*id))),
            Self::Val(o) => o.iter(),
            Self::Empty => Box::new(std::iter::empty()),
        }
    }
}
