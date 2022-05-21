pub mod query;

use query::{Query, Operator};

fn main() {
    let query = Query::scan("example").filter("id", Operator::EQ, "1").select(&["id", "a_column"]);
    println!("{}", query);
}
