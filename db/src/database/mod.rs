mod insert;
mod obj_view;
mod db;
mod define;

use core::fmt;
use std::{cell::RefCell, rc::Rc};

pub use crate::database::db::Database;
use crate::object::IndexedObject;

// TODO: Use a proper error type
pub(crate) type Result<T> = std::result::Result<T, Error>;
pub(crate) type SharedObject = Rc<RefCell<IndexedObject>>;

#[derive(Debug)]
pub struct Error(Box<str>);

impl std::error::Error for Error {}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl From<String> for Error {
    fn from(s: String) -> Self {
        Error(Box::from(s))
    }
}

impl From<&str> for Error {
    fn from(s: &str) -> Self {
        Error(Box::from(s))
    }
}
