mod insert;
mod obj_view;
mod db;

use std::{cell::RefCell, rc::Rc};

pub use crate::database::db::Database;
use crate::object::IndexedObject;

pub(crate) type Result<T> = std::result::Result<T, String>;
pub(crate) type SharedObject = Rc<RefCell<IndexedObject>>;
