use crate::{schema::{AttributeIdentifier, AttributeRef}, Database};

impl Database {
    pub fn attribute(&self, id: impl AttributeIdentifier) -> Option<AttributeRef> {
        self.schema.lookup_attribute(id).map(|attr| attr.reference())
    }
}
