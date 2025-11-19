pub mod fixture;
mod r#gen;
mod db_ext;

#[macro_export]
macro_rules! hash_set {
    ($e:expr_2021) => {
        {
            let mut h = std::collections::hash_set::HashSet::new();
            h.insert($e);
            h
        }
    };
    ($e:expr_2021, $($rest:expr_2021), +) => {
        {
            let mut h = hash_set!($($rest), +);
            h.insert($e);
            h
        }
    };
}
