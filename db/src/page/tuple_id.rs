use std::{fmt, ops::{Index, IndexMut}};

#[derive(Copy, Clone, Hash, PartialEq, Eq, Debug)]
pub(crate) struct TupleId {
    pub(crate) block: u32,
    pub(crate) offset: u32,
}

impl TupleId {
    pub(crate) fn anonymous() -> Self {
        Self { block: 0, offset: 0 }
    }

    pub(crate) fn is_anonymous(&self) -> bool {
        self.offset == 0
    }
}

impl fmt::Display for TupleId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "TupleId({}, {})", self.block, self.offset)
    }
}

impl Index<TupleId> for Vec<Vec<u8>> {
    type Output = Vec<u8>;

    fn index(&self, index: TupleId) -> &Self::Output {
        &self[(index.offset - 1) as usize]
    }
}

impl IndexMut<TupleId> for Vec<Vec<u8>> {
    fn index_mut(&mut self, index: TupleId) -> &mut Self::Output {
        &mut self[(index.offset - 1) as usize]
    }
}

impl From<usize> for TupleId {
    fn from(value: usize) -> Self {
        Self {
            block: 0,
            offset: <u32>::try_from(value).unwrap() + 1,
        }
    }
}

impl From<&usize> for TupleId {
    fn from(value: &usize) -> Self {
        Self {
            block: 0,
            offset: <u32>::try_from(*value).unwrap() + 1,
        }
    }
}

impl From<&TupleId> for u32 {
    fn from(value: &TupleId) -> Self {
        value.offset - 1
    }
}

impl From<TupleId> for usize {
   fn from(value: TupleId) -> Self {
        value.offset as usize - 1
    }
}
