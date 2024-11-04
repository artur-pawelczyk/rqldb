pub mod fixture;
mod gen;
mod db_ext;

#[macro_export]
macro_rules! hash_set {
    ($e:expr) => {
        {
            let mut h = std::collections::hash_set::HashSet::new();
            h.insert($e);
            h
        }
    };
    ($e:expr, $($rest:expr), +) => {
        {
            let mut h = hash_set!($($rest), +);
            h.insert($e);
            h
        }
    };
}
