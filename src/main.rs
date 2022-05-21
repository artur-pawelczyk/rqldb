pub mod select;
pub mod create;

use select::{SelectQuery, Operator};

fn main() {
    let query = SelectQuery::scan("example").filter("id", Operator::EQ, "1").select(&["id", "a_column"]);
    println!("{}", query);
}
