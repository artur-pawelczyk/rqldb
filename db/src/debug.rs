use std::fmt;

use crate::Tuple;

pub struct DebugTuple<'a>(Tuple<'a>);

impl<'a> fmt::Debug for DebugTuple<'a> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        for attr in self.0.attributes() {
            todo!()
        }

        Ok(())
    }
}
