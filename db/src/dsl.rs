use std::{collections::BTreeMap, fmt, str::FromStr};

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

    pub fn tuple(values: impl IntoTuple<'a>) -> Self {
        Self {
            source: Source::Tuple(values.into_tuple()),
            join_sources: vec![],
            filters: Vec::new(),
            finisher: Finisher::AllColumns,
        }
    }

    pub fn join(mut self, left: &'a str, right: &'a str) -> Self {
        self.join_sources.push(JoinSource{ left, right });
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

    pub fn apply(mut self, function: &'a str, args: &[&'a str]) -> Self {
        self.finisher = Finisher::Apply(function, args.to_vec());
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
}

impl<'a> fmt::Display for Query<'a> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", &self.source)?;
        for join in &self.join_sources {
            write!(f, " | {}", join)?;
        }

        for filter in &self.filters {
            write!(f, " | {}", filter)?;
        }

        write!(f, " | {}", self.finisher)?;

        Ok(())
    }
}
    
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Operator {
    EQ, GT, GE, LT, LE
}

impl fmt::Display for Operator {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", match self {
            Operator::EQ => "=",
            Operator::GT => ">",
            Operator::GE => ">=",
            Operator::LT => "<",
            Operator::LE => "<=",
        })
    }
}

impl FromStr for Operator {
    type Err = ParseError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "=" => Ok(Self::EQ),
            _ => Err(ParseError::msg("Operator not recognized")),
        }
    }
}

#[derive(Debug, Default, Eq, PartialEq)]
pub enum Source<'a> {
    #[default]
    Nil,
    TableScan(&'a str),
    IndexScan(&'a str, Operator, &'a str),
    Tuple(Vec<TupleAttr<'a>>),
}

#[non_exhaustive]
#[derive(PartialEq, Eq, Debug, Default, Copy, Clone)]
// TODO: Should it be replaced with lib::Type?
pub enum AttrKind {
    #[default] Infer,
    Number,
    Text,
}

impl FromStr for AttrKind {
    type Err = ();

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "NUMBER" => Ok(Self::Number),
            "TEXT" => Ok(Self::Text),
            _ => Err(()),
        }
    }
}

impl From<AttrKind> for Type {
    fn from(kind: AttrKind) -> Self {
        match kind {
            AttrKind::Number => Self::NUMBER,
            AttrKind::Text => Self::TEXT,
            _ => Self::NONE,
        }
    }
}

impl fmt::Display for AttrKind {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Self::Infer => write!(f, "INFER"),
            Self::Number => write!(f, "NUMBER"),
            Self::Text => write!(f, "TEXT"),
        }
    }
}

#[derive(Clone, PartialEq, Eq, Debug)]
pub struct TupleAttr<'a> {
    pub kind: AttrKind,
    pub name: Box<str>,
    pub value: &'a str,
}

pub trait IntoTuple<'a> {
    fn into_tuple(self) -> Vec<TupleAttr<'a>>;
}

impl<'a> IntoTuple<'a> for &[(&'a str, &'a str)] {
    fn into_tuple(self) -> Vec<TupleAttr<'a>> {
        self.iter().fold(TupleBuilder::new(), |b, (k, v)| b.inferred(k, v)).into_tuple()
    }
}

impl<'a> IntoTuple<'a> for &'a [(&'a str, String)] {
    fn into_tuple(self) -> Vec<TupleAttr<'a>> {
        self.iter().fold(TupleBuilder::new(), |b, (k, v)| b.inferred(k, v)).into_tuple()
    }
}

impl<'a, const N: usize> IntoTuple<'a> for &[(&'a str, &'a str); N] {
    fn into_tuple(self) -> Vec<TupleAttr<'a>> {
        self.iter().fold(TupleBuilder::new(), |b, (k, v)| b.inferred(k, v)).into_tuple()        
    }
}

impl<'a> IntoTuple<'a> for &[TupleAttr<'a>] {
    fn into_tuple(self) -> Vec<TupleAttr<'a>> {
        self.to_vec()
    }
}

impl<'a> IntoTuple<'a> for Vec<TupleAttr<'a>> {
    fn into_tuple(self) -> Vec<TupleAttr<'a>> {
        self
    }
}

impl<'a> IntoTuple<'a> for &Vec<TupleAttr<'a>> {
    fn into_tuple(self) -> Vec<TupleAttr<'a>> {
        self.to_vec()
    }
}

impl<'a> IntoTuple<'a> for &'a BTreeMap<&str, String> {
    fn into_tuple(self) -> Vec<TupleAttr<'a>> {
        self.iter().fold(TupleBuilder::new(), |b, (k, v)| b.inferred(k, v)).into_tuple()
    }
}

impl<'a> IntoTuple<'a> for &'a BTreeMap<&str, &str> {
    fn into_tuple(self) -> Vec<TupleAttr<'a>> {
        self.iter().fold(TupleBuilder::new(), |b, (k, v)| b.inferred(k, v)).into_tuple()
    }
}

#[derive(Default)]
pub struct TupleBuilder<'a>(Vec<TupleAttr<'a>>);

impl<'a> TupleBuilder<'a> {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn typed(mut self, kind: AttrKind, name: &'a str, value: &'a str) -> Self {
        self.0.push(TupleAttr { name: Box::from(name), kind, value });
        self
    }
    
    pub fn inferred(mut self, name: &'a str, value: &'a str) -> Self {
        self.0.push(TupleAttr { name: Box::from(name), kind: AttrKind::Infer, value });
        self
    }
}

impl<'a> IntoTuple<'a> for TupleBuilder<'a> {
    fn into_tuple(self) -> Vec<TupleAttr<'a>> {
        self.0
    }
}

impl<'a> fmt::Display for Source<'a> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Source::Nil => write!(f, "nil"),
            Source::TableScan(table) => write!(f, "scan {}", table),
            Source::IndexScan(index, _, val) => write!(f, "scan_index {} = {}", index, val),
            Source::Tuple(values) => { write!(f, "tuple ")?; write_attrs(f, values) },
        }
    }
}

#[derive(Debug, Eq, PartialEq)]
pub struct JoinSource<'a> {
    pub left: &'a str,
    pub right: &'a str,
}

impl<'a> fmt::Display for JoinSource<'a> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "join {} {}", self.left, self.right)
    }
}

#[derive(Debug, Eq, PartialEq)]
pub enum Filter<'a> {
    Condition(&'a str, Operator, &'a str),
}

impl<'a> fmt::Display for Filter<'a> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Filter::Condition(left, op, right) => write!(f, "filter {} {} {}", left, op, right),
        }
    }
}

#[derive(Debug, Default, Eq, PartialEq)]
pub enum Finisher<'a> {
    #[default]
    AllColumns,
    Columns(Vec<&'a str>),
    Apply(&'a str, Vec<&'a str>),
    Insert(&'a str),
    Count,
    Delete,
}

impl<'a> fmt::Display for Finisher<'a> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Finisher::AllColumns => write!(f, "select_all"),
            Finisher::Columns(rows) => { write!(f, "select ")?; write_tokens(f, rows) },
            Finisher::Apply(fun, a) => { write!(f, "apply {} ", fun)?; write_tokens(f, a) }
            Finisher::Insert(name) => write!(f, "insert_into {}", name),
            Finisher::Count => write!(f, "count"),
            Finisher::Delete => write!(f, "delete"),
        }
    }
}

fn write_tokens(f: &mut fmt::Formatter, tokens: &[&str]) -> fmt::Result {
    let mut i = tokens.iter().peekable();
    while let Some(token) = i.next() {
        if token.contains(' ') {
            write!(f, "\"{}\"", token)?;
        } else {
            write!(f, "{}" , token)?;
        }

        if i.peek().is_some() {
            write!(f, " ")?;
        }
    }

    Ok(())
}

fn write_attrs(f: &mut fmt::Formatter, attrs: &[TupleAttr<'_>]) -> fmt::Result {
    let mut i = attrs.iter().peekable();
    while let Some(attr) = i.next() {
        if attr.name.chars().next().map(|c| !c.is_numeric()).unwrap_or(false) {
            if attr.kind == AttrKind::Infer {
                write!(f, "{} = ", attr.name)?;
            } else {
                write!(f, "{}::{} = ", attr.name, attr.kind)?;
            }
        }

        if attr.value.contains(' ') {
            write!(f, "\"{}\"", attr.value)?;
        } else {
            write!(f, "{}" , attr.value)?;
        }

        if i.peek().is_some() {
            write!(f, " ")?;
        }
    }

    Ok(())
}

#[derive(Eq, PartialEq, Debug)]
pub struct Definition {
    pub name: String,
    pub columns: Vec<Column>
}

impl Definition {
    pub fn relation(name: &str) -> Self {
        Definition{name: name.to_string(), columns: Vec::new()}
    }

    pub fn attribute(mut self, name: &str, kind: Type) -> Self {
        self.columns.push(Column{ name: name.to_string(), kind, indexed: false });
        self
    }

    pub fn indexed_attribute(mut self, name: &str, kind: Type) -> Self {
        self.columns.push(Column{ name: name.to_string(), kind, indexed: true });
        self
    }
}

impl fmt::Display for Definition {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "relation {} ", self.name)?;
        let mut i = self.columns.iter().peekable();
        while let Some(col) = i.next() {
            write!(f, "{}", col)?;
            if i.peek().is_some() {
                write!(f, " ")?;
            }
        }

        Ok(())
    }
}

#[derive(Eq, PartialEq, Debug)]
pub struct Column {
    pub name: String,
    pub kind: Type,
    pub indexed: bool,
}

impl fmt::Display for Column {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        if self.indexed {
            write!(f, "{}::{}::KEY", self.name, self.kind)
        } else {
            write!(f, "{}::{}", self.name, self.kind)
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
        let query = Query::tuple(&[("id", "1"), ("value", "example_value")]).insert_into("example");
        assert_eq!(query.to_string(), "tuple id = 1 value = example_value | insert_into example");
    }

    #[test]
    fn join() {
        let query = Query::scan("example").join("example.type_id", "type.id");
        assert_eq!(query.to_string(), "scan example | join example.type_id type.id | select_all");
    }

    #[test]
    fn count() {
        let query = Query::scan("example").count();
        assert_eq!(query.to_string(), "scan example | count");
    }

    #[test]
    fn apply() {
        let query = Query::scan("example").apply("sum", &["example.n"]);
        assert_eq!(query.to_string(), "scan example | apply sum example.n");
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
    fn define_relation() {
        let query = Definition::relation("example")
            .indexed_attribute("id", Type::NUMBER)
            .attribute("contents", Type::TEXT);

        assert_eq!("relation example id::NUMBER::KEY contents::TEXT", query.to_string());
    }

    #[test]
    fn quotes() {
        let query = Query::tuple(&[("id", "1"), ("value", "foo bar")]).insert_into("example");

        assert_eq!("tuple id = 1 value = \"foo bar\" | insert_into example", query.to_string());
    }
}
