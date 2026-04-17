use std::{num::NonZero, ops::Add};

#[derive(Clone, Copy, PartialEq, PartialOrd)]
pub(crate) struct TransactionId(Option<NonZero<u32>>);

impl TransactionId {
    pub(crate) fn nil() -> Self {
        Self(None)
    }
}

impl Default for TransactionId {
    fn default() -> Self {
        Self(NonZero::new(1))
    }
}

impl From<TransactionId> for [u8; 4] {
    fn from(value: TransactionId) -> Self {
        value.0.map(NonZero::get).unwrap_or(0).to_be_bytes()
    }
}

impl From<[u8; 4]> for TransactionId {
    fn from(value: [u8; 4]) -> Self {
        let id = u32::from_be_bytes(value);
        Self(NonZero::new(id))
    }
}
    
impl Add<u32> for TransactionId {
    type Output = Self;

    fn add(self, rhs: u32) -> Self::Output {
        Self(self.0.map(|n| n.saturating_add(rhs)))
    }
}
