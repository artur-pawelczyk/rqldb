use std::{fmt, ops::{Index, IndexMut}};

#[derive(Copy, Clone, Hash, PartialEq, Eq, Debug)]
pub(crate) struct TupleId(pub u32);

impl TupleId {
    pub(crate) fn anonymous() -> Self {
        Self(0)
    }

    pub(crate) fn is_anonymous(&self) -> bool {
        self.0 == 0
    }
}

impl fmt::Display for TupleId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "TupleId({})", self.0)
    }
}

impl Index<TupleId> for Vec<Vec<u8>> {
    type Output = Vec<u8>;

    fn index(&self, index: TupleId) -> &Self::Output {
        &self[(index.0 - 1) as usize]
    }
}

impl IndexMut<TupleId> for Vec<Vec<u8>> {
    fn index_mut(&mut self, index: TupleId) -> &mut Self::Output {
        &mut self[(index.0 - 1) as usize]
    }
}

impl From<usize> for TupleId {
    fn from(value: usize) -> Self {
        Self(<u32>::try_from(value).unwrap() + 1)
    }
}

impl From<&usize> for TupleId {
    fn from(value: &usize) -> Self {
        Self(<u32>::try_from(*value).unwrap() + 1)
    }
}

impl From<&TupleId> for u32 {
    fn from(value: &TupleId) -> Self {
        value.0
    }
}
