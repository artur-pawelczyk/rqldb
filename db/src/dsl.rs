use std::fmt;

use crate::schema::{Column, Type};

pub struct Query {
    pub source: Source,
    pub join_sources: Vec<JoinSource>,
    pub filters: Vec<Filter>,
    pub finisher: Finisher
}

impl Query {
    pub fn scan(table: &str) -> Self {
        Self{source:  Source::TableScan(String::from(table)), join_sources: vec![], filters: vec![], finisher: Finisher::AllColumns}
    }

    pub fn tuple<T: ToString>(values: &[T]) -> Self {
        Self{source: Source::Tuple(values.iter().map(|x| x.to_string()).collect()), join_sources: vec![], filters: Vec::new(), finisher: Finisher::AllColumns}
    }

    pub fn join(mut self, table: &str, left: &str, right: &str) -> Self {
        self.join_sources.push(JoinSource{table: table.to_string(), left: left.to_string(), right: right.to_string()});
        self
    }

    pub fn filter(mut self, left: &str, op: Operator, right: &str) -> Self {
        self.filters.push(Filter::Condition(left.to_string(), op, right.to_string()));
        self
    }

    pub fn select_all(mut self) -> Self {
        self.finisher = Finisher::AllColumns;
        self
    }

    pub fn select(mut self, columns: &[&str]) -> Self {
        self.finisher = Finisher::Columns(columns.iter().map(|x| x.to_string()).collect());
        self
    }

    pub fn insert_into(mut self, name: &str) -> Self {
        self.finisher = Finisher::Insert(name.to_string());
        self
    }

    pub fn count(mut self) -> Self {
        self.finisher = Finisher::Count;
        self
    }

    pub fn delete(mut self) -> Self {
        self.finisher = Finisher::Delete;
        self
    }

    pub fn print(&self) -> String {
        let mut s = String::new();

        s.push_str(&self.source.print());

        for join in &self.join_sources {
            s.push_str(" | ");
            s.push_str(&join.print());
        }

        for filter in &self.filters {
            s.push_str(" | ");
            s.push_str(&filter.print());
        }

        s.push_str(" | ");
        s.push_str(&self.finisher.print());

        s
    }
}

impl fmt::Display for Query {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.print())
    }
}
    
#[derive(Clone, Copy, Debug, PartialEq)]
pub enum Operator {
    EQ, GT, GE, LT, LE
}

impl Operator {
    fn print(&self) -> String {
        match self {
            Operator::EQ => "=",
            Operator::GT => ">",
            Operator::GE => ">=",
            Operator::LT => "<",
            Operator::LE => "<=",
        }.to_string()
    }
}

pub enum Source {
    TableScan(String),
    Tuple(Vec<String>)
}

impl Source {
    fn print(&self) -> String {
        match self {
            Source::TableScan(table) => "scan ".to_owned() + table,
            Source::Tuple(values) => "tuple ".to_owned() + &print_tokens(values)
        }
    }
}

pub struct JoinSource {
    pub table: String,
    pub left: String,
    pub right: String,
}

impl JoinSource {
    fn print(&self) -> String {
        "join".to_string()
            + " " + self.table.as_str()
            + " " + self.left.as_str()
            + " " + self.right.as_str()
    }
}

pub enum Filter {
    Condition(String, Operator, String)
}

impl Filter {
    fn print(&self) -> String {
        match self {
            Filter::Condition(left, op, right) => "filter ".to_owned() + left + " " + &op.print() + " " + right
        }
    }
}

pub enum Finisher {
    AllColumns,
    Columns(Vec<String>),
    Insert(String),
    Count,
    Delete,
}

impl Finisher {
    fn print(&self) -> String {
        match self {
            Finisher::AllColumns => "select_all".to_string(),
            Finisher::Columns(rows) => "select ".to_string() + &print_tokens(rows),
            Finisher::Insert(name) => "insert_into ".to_string() + name,
            Finisher::Count => "count".to_string(),
            Finisher::Delete => "delete".to_string(),
        }
    }
}

fn print_tokens(tokens: &[String]) -> String {
    let mut s  = String::new();
    for token in tokens {
        s.push_str(token);
        s.push(' ');
    }

    if !s.is_empty() { s.pop(); }
    
    s
}

pub struct Command {
    pub name: String,
    pub columns: Vec<Column>
}

impl Command {
    pub fn create_table(name: &str) -> Self {
        Command{name: name.to_string(), columns: Vec::new()}
    }

    pub fn column(mut self, name: &str, kind: Type) -> Self {
        self.columns.push(Column{name: name.to_string(), kind});
        self
    }

    pub fn print(&self) -> String {
        let mut s = String::new();
        s.push_str("create_table ");
        s.push_str(&self.name);
        s.push(' ');

        for col in &self.columns {
            s.push_str(&col.print());
            s.push(' ');
        }
        s.pop();

        s
    }
}

impl fmt::Display for Command {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.print())
    }
}

impl Column {
    fn print(&self) -> String {
        self.name.clone() + "::" + self.kind.print()
    }
}

impl Type {
    fn print(&self) -> &'static str {
        match self {
            Type::NUMBER => "NUMBER",
            Type::TEXT => "TEXT",
            Type::BYTE(n) => if n == &1 { "UINT8" } else if n == &2 { "UINT16" } else if n == &4 { "UINT36" } else { panic!() },
            Type::NONE => panic!(),
        }
    }
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
    fn filter() {
        assert_eq!(
            Query::scan("example").filter("id", EQ, "1").select(&["id", "a_column"]).to_string(),
            "scan example | filter id = 1 | select id a_column");
        assert_eq!(
            Query::scan("example").filter("id", GT, "2").select_all().to_string(), "scan example | filter id > 2 | select_all"
        );
    }

    #[test]
    fn source_is_tuple() {
        let query = Query::tuple(&["1", "example_value"]).insert_into("example");
        assert_eq!(query.to_string(), "tuple 1 example_value | insert_into example");
    }

    #[test]
    fn join() {
        let query = Query::scan("example").join("type", "example.type_id", "type.id");
        assert_eq!(query.to_string(), "scan example | join type example.type_id type.id | select_all");
    }

    #[test]
    fn count() {
        let query = Query::scan("example").count();
        assert_eq!(query.to_string(), "scan example | count");
    }

    #[test]
    fn delete() {
        let delete_all = Query::scan("example").delete();
        assert_eq!(delete_all.to_string(), "scan example | delete");

        let delete_one = Query::scan("example").filter("example.id", EQ, "1").delete();
        assert_eq!(delete_one.to_string(), "scan example | filter example.id = 1 | delete");
    }


    #[test]
    fn create_table() {
        let query = Command::create_table("example")
            .column("id", Type::NUMBER)
            .column("contents", Type::TEXT);

        assert_eq!("create_table example id::NUMBER contents::TEXT", query.to_string());
    }
}
