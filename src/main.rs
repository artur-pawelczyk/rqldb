use std::fmt;

struct Query {}

struct SelectQuery {
    source: String,
    conditions: Vec<Condition>,
    columns: Vec<String>
}

impl Query {
    pub fn scan(table: &str) -> SelectQuery {
        SelectQuery{source:  String::from(table), conditions: Vec::new(), columns: Vec::new()}
    }
}

impl SelectQuery {
    pub fn filter(mut self, left: &str, op: Operator, right: &str) -> Self {
        self.conditions.push(Condition{});
        return self;
    }

    pub fn select_all(mut self) -> Self {
        self.columns.clear();
        self
    }

    pub fn select(mut self, columns: &[&str]) -> Self {
        self.columns.reserve(columns.len());
        for col in columns {
            self.columns.push(col.to_string());
        }
        self
    }

    pub fn print(&self) -> String {
        let mut s = String::new();
        s.push_str("scan ");
        s.push_str(&self.source);
        s.push_str(" | ");
        if self.columns.is_empty() {
            s.push_str("select_all");
        } else {
            s.push_str("select ");
            for col in &self.columns {
                s.push_str(&col);
                s.push(' ');
            }
            s.pop();
        }

        s
    }
}

impl fmt::Display for SelectQuery {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.print())
    }
}
    

enum Operator {
    EQ
}

struct Condition {}

fn main() {
    let query = Query::scan("example").filter("id", Operator::EQ, "1").select(&["id", "a_column"]);
    println!("{}", query);
}

#[cfg(test)]
mod tests {
    use super::*;
    use super::Operator::*;

    #[test]
    fn select_all() {
        let query = Query::scan("example").select_all();
        assert_eq!(query.to_string(), "scan example | select_all")
    }

    #[test]
    fn where_clause() {
        let query = Query::scan("example").filter("id", EQ, "1").select(&["id", "a_column"]);
        assert_eq!(query.to_string(), "scan example | select id a_column")
    }
}
