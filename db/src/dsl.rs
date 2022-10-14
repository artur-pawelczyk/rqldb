use std::{fmt, str::FromStr};

use crate::{schema::Type, parse::ParseError};

#[derive(Debug, Default, Eq, PartialEq)]
pub struct Query<'a> {
    pub source: Source<'a>,
    pub join_sources: Vec<JoinSource<'a>>,
    pub filters: Vec<Filter<'a>>,
    pub finisher: Finisher<'a>
}

impl<'a> Query<'a> {
    pub fn scan(table: &'a str) -> Self {
        Self{
            source:  Source::TableScan(table),
            ..Self::default()
        }
    }

    pub fn scan_index(index: &'a str, op: Operator, val: &'a str) -> Self {
        Self{
            source: Source::IndexScan(index, op, val),
            ..Self::default()
        }
    }

    pub fn tuple(values: &[&'a str]) -> Self {
        Self{source: Source::Tuple(values.to_vec()), join_sources: vec![], filters: Vec::new(), finisher: Finisher::AllColumns }
    }

    pub fn join(mut self, table: &'a str, left: &'a str, right: &'a str) -> Self {
        self.join_sources.push(JoinSource{ table, left, right });
        self
    }

    pub fn filter(mut self, left: &'a str, op: Operator, right: &'a str) -> Self {
        self.filters.push(Filter::Condition(left, op, right));
        self
    }

    pub fn select_all(mut self) -> Self {
        self.finisher = Finisher::AllColumns;
        self
    }

    pub fn select(mut self, columns: &[&'a str]) -> Self {
        self.finisher = Finisher::Columns(columns.to_vec());
        self
    }

    pub fn insert_into(mut self, name: &'a str) -> Self {
        self.finisher = Finisher::Insert(name);
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

impl<'a> fmt::Display for Query<'a> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.print())
    }
}
    
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
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

impl FromStr for Operator {
    type Err = ParseError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "=" => Ok(Self::EQ),
            _ => Err(ParseError("Operator not recognized")),
        }
    }
}

#[derive(Debug, Default, Eq, PartialEq)]
pub enum Source<'a> {
    #[default]
    Nil,
    TableScan(&'a str),
    IndexScan(&'a str, Operator, &'a str),
    Tuple(Vec<&'a str>),
}

impl<'a> Source<'a> {
    fn print(&self) -> String {
        match self {
            Source::Nil => "nil".to_string(),
            Source::TableScan(table) => "scan ".to_string() + table,
            Source::IndexScan(index, _, val) => format!("scan_index {} = {}", index, val),
            Source::Tuple(values) => "tuple ".to_string() + &print_tokens(values)
        }
    }
}

#[derive(Debug, Eq, PartialEq)]
pub struct JoinSource<'a> {
    pub table: &'a str,
    pub left: &'a str,
    pub right: &'a str,
}

impl<'a> JoinSource<'a> {
    fn print(&self) -> String {
        "join".to_string()
            + " " + self.table
            + " " + self.left
            + " " + self.right
    }
}

#[derive(Debug, Eq, PartialEq)]
pub enum Filter<'a> {
    Condition(&'a str, Operator, &'a str),
}

impl<'a> Filter<'a> {
    fn print(&self) -> String {
        match self {
            Filter::Condition(left, op, right) => "filter ".to_owned() + left + " " + &op.print() + " " + right
        }
    }
}

#[derive(Debug, Default, Eq, PartialEq)]
pub enum Finisher<'a> {
    #[default]
    AllColumns,
    Columns(Vec<&'a str>),
    Insert(&'a str),
    Count,
    Delete,
}

impl<'a> Finisher<'a> {
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

fn print_tokens(tokens: &[&str]) -> String {
    let mut s  = String::new();

    for token in tokens {
        let quote = token.contains(' ');
        if quote {
            s.push('"');
        }

        s.push_str(token);

        if quote {
            s.push('"');
        }

        s.push(' ');

    }

    if !s.is_empty() { s.pop(); }
    
    s
}

#[derive(Eq, PartialEq, Debug)]
pub struct Command {
    pub name: String,
    pub columns: Vec<Column>
}

impl Command {
    pub fn create_table(name: &str) -> Self {
        Command{name: name.to_string(), columns: Vec::new()}
    }

    pub fn column(mut self, name: &str, kind: Type) -> Self {
        self.columns.push(Column{ name: name.to_string(), kind, indexed: false });
        self
    }

    pub fn indexed_column(mut self, name: &str, kind: Type) -> Self {
        self.columns.push(Column{ name: name.to_string(), kind, indexed: true });
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

#[derive(Eq, PartialEq, Debug)]
pub struct Column {
    pub name: String,
    pub kind: Type,
    pub indexed: bool,
}

impl Column {
    fn print(&self) -> String {
        if self.indexed {
            self.name.clone() + "::" + self.kind.print() + "::KEY"
        } else {
            self.name.clone() + "::" + self.kind.print()
        }
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
    fn index_scan() {
        let query = Query::scan_index("example.id", Operator::EQ, "1");
        assert_eq!(query.to_string(), "scan_index example.id = 1 | select_all");
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
            .indexed_column("id", Type::NUMBER)
            .column("contents", Type::TEXT);

        assert_eq!("create_table example id::NUMBER::KEY contents::TEXT", query.to_string());
    }

    #[test]
    fn quotes() {
        let query = Query::tuple(&["1", "foo bar"]).insert_into("example");

        assert_eq!("tuple 1 \"foo bar\" | insert_into example", query.to_string());
    }
}
