mod insert;
mod obj_view;
mod db;
mod define;

use std::{cell::RefCell, rc::Rc};

pub use crate::database::db::Database;
use crate::object::IndexedObject;

// TODO: Use a proper error type
pub(crate) type Result<T> = std::result::Result<T, String>;
pub(crate) type SharedObject = Rc<RefCell<IndexedObject>>;
