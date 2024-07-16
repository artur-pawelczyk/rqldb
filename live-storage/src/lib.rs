use std::path::Path;

use rqldb::Database;

pub struct LiveStorage {
    dir: Path
}

impl LiveStorage {
    fn create_db(&self) -> Database {
        todo!()
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn test_something() {
        assert!(true);
    }
}
