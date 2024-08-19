use crate::bytes::write_as_bytes;
use crate::object::NamedAttribute as _;
use crate::{dsl, Database};
use crate::database::Result;

impl Database {
    pub fn insert(&self, cmd: &dsl::Insert<'_, Vec<dsl::TupleAttr<'_>>>) -> Result<()> {
        let mut target = self.object(cmd.target)
            .ok_or_else(|| format!("Relation {} not found", cmd.target))?
            .borrow_mut();

        let mut byte_tuple = Vec::new();
        for attr in target.attributes() {
            let elem = cmd.tuple
                .iter().find(|elem| elem.name == attr.name.as_ref() || elem.name == attr.short_name())
                .ok_or_else(|| format!("Attribute {} is missing", attr.name))?;

            write_as_bytes(attr.kind, &elem.value, &mut byte_tuple)
                .map_err(|_| format!("Error encoding value for attribute {}", attr.name))?;
        }

        target.add_tuple(&byte_tuple);

        Ok(())
    }
}
