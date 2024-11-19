use std::{borrow::Cow, collections::BTreeMap, fmt, str::FromStr};

use crate::{parse::ParseError, schema::Type};

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct Query<'a> {
    pub source: Source<'a>,
    pub join_sources: Vec<JoinSource<'a>>,
    pub(crate) mappers: Vec<Mapper<'a>>,
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
            ..Default::default()
        }
    }

    pub fn build_tuple() -> TupleBuilder<'a> {
        TupleBuilder::new()
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

    pub fn set(mut self, attr: &'a str, value: impl Encode<'a>) -> Self {
        self.mappers.push(Mapper {
            function: "set",
            args: Box::from([attr.into(), value.encode()]),
        });
        self
    }

    pub fn insert_into(self, name: &'a str) -> Insert<'a> {
        Insert { target: name, contents: self }

    }

    pub fn count(mut self) -> Self {
        self.finisher = Finisher::Count;
        self
    }

    pub fn delete(mut self) -> Delete<'a> {
        self.finisher = Finisher::AllColumns;
        Delete(self)
    }
}

impl<'a> fmt::Display for Query<'a> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", &self.source)?;
        for join in &self.join_sources {
            write!(f, " | {}", join)?;
        }

        for mapper in &self.mappers {
            write!(f, " | {}", mapper)?;
        }

        for filter in &self.filters {
            write!(f, " | {}", filter)?;
        }

        if !self.finisher.is_default() {
            write!(f, " | {}", self.finisher)?;
        }

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

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub enum Source<'a> {
    #[default]
    Nil,
    TableScan(&'a str),
    IndexScan(&'a str, Operator, &'a str),
    Tuple(Vec<TupleAttr<'a>>),
}

#[derive(Clone, PartialEq, Eq, Debug)]
pub struct TupleAttr<'a> {
    pub kind: Option<Type>,
    pub name: &'a str,
    pub value: Cow<'a, str>,
}

pub trait IntoTuple<'a> {
    fn into_tuple(self) -> Vec<TupleAttr<'a>>;
}

impl<'a> IntoTuple<'a> for &[(&'a str, &'a str)] {
    fn into_tuple(self) -> Vec<TupleAttr<'a>> {
        self.iter().map(|(k, v)| TupleAttr { kind: None, name: k, value: Cow::Borrowed(v) }).collect()
    }
}

impl<'a> IntoTuple<'a> for &'a [(&'a str, String)] {
    fn into_tuple(self) -> Vec<TupleAttr<'a>> {
        self.iter().map(|(k, v)| TupleAttr { kind: None, name: k, value: Cow::Borrowed(v) }).collect()
    }
}

impl<'a, const N: usize> IntoTuple<'a> for &[(&'a str, &'a str); N] {
    fn into_tuple(self) -> Vec<TupleAttr<'a>> {
        self.iter().map(|(k, v)| TupleAttr { kind: None, name: k, value: Cow::Borrowed(v) }).collect()
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
        self.iter().map(|(k, v)| TupleAttr { kind: None, name: k, value: Cow::Borrowed(v) }).collect()
    }
}

impl<'a> IntoTuple<'a> for &'a BTreeMap<&str, &str> {
    fn into_tuple(self) -> Vec<TupleAttr<'a>> {
        self.iter().map(|(k, v)| TupleAttr { kind: None, name: k, value: Cow::Borrowed(v) }).collect()
    }
}

#[derive(Default)]
pub struct TupleBuilder<'a>(Vec<TupleAttr<'a>>);

impl<'a> TupleBuilder<'a> {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn build(self) -> Query<'a> {
        Query::tuple(self)
    }

    pub fn typed(mut self, kind: Type, name: &'a str, value: impl Encode<'a>) -> Self {
        self.0.push(TupleAttr { name, kind: Some(kind), value: value.encode() });
        self
    }
    
    pub fn inferred(mut self, name: &'a str, value: impl Encode<'a>) -> Self {
        self.0.push(TupleAttr { name, kind: None, value: value.encode() });
        self
    }
}

pub trait Encode<'a>
where Self: Sized
{
    fn encode(self) -> Cow<'a, str>;
}

impl<'a> Encode<'a> for &'a str {
    fn encode(self) -> Cow<'a, str> {
        Cow::Borrowed(self)
    }
}

impl<'a> Encode<'a> for String {
    fn encode(self) -> Cow<'a, str> {
        Cow::Owned(self)
    }
}

impl<'a> Encode<'a> for u32 {
    fn encode(self) -> Cow<'a, str> {
        Cow::Owned(self.to_string())
    }
}

impl<'a> Encode<'a> for i32 {
    fn encode(self) -> Cow<'a, str> {
        Cow::Owned(self.to_string())
    }
}

impl<'a> Encode<'a> for usize {
    fn encode(self) -> Cow<'a, str> {
        Cow::Owned(self.to_string())
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

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct JoinSource<'a> {
    pub left: &'a str,
    pub right: &'a str,
}

impl<'a> fmt::Display for JoinSource<'a> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "join {} {}", self.left, self.right)
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct Mapper<'a> {
    pub(crate) function: &'a str,
    pub(crate) args: Box<[Cow<'a, str>]>,
}

impl<'a> fmt::Display for Mapper<'a> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "map {} ", self.function)?;

        write_tokens(f, &self.args)
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
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

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub enum Finisher<'a> {
    #[default]
    AllColumns,
    Columns(Vec<&'a str>),
    Apply(&'a str, Vec<&'a str>),
    Count,
}

impl Finisher<'_> {
    fn is_default(&self) -> bool {
        match self {
            Self::AllColumns => true,
            _ => false,
        }
    }
}

impl<'a> fmt::Display for Finisher<'a> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Finisher::AllColumns => write!(f, "select_all"),
            Finisher::Columns(rows) => { write!(f, "select ")?; write_tokens(f, rows) },
            Finisher::Apply(fun, a) => { write!(f, "apply {} ", fun)?; write_tokens(f, a) }
            Finisher::Count => write!(f, "count"),
        }
    }
}

fn write_tokens(f: &mut fmt::Formatter, tokens: &[impl AsRef<str>]) -> fmt::Result {
    let mut i = tokens.iter().peekable();
    while let Some(token) = i.next() {
        if token.as_ref().contains(' ') {
            write!(f, "\"{}\"", token.as_ref())?;
        } else {
            write!(f, "{}" , token.as_ref())?;
        }

        if i.peek().is_some() {
            write!(f, " ")?;
        }
    }

    Ok(())
}

// TODO: Use 'write_tokens' internally
fn write_attrs(f: &mut fmt::Formatter, attrs: &[TupleAttr<'_>]) -> fmt::Result {
    let mut i = attrs.iter().peekable();
    while let Some(attr) = i.next() {
        if attr.name.chars().next().map(|c| !c.is_numeric()).unwrap_or(false) {
            if let Some(kind) = attr.kind {
                write!(f, "{}::{} = ", attr.name, kind)?;
            } else {
                write!(f, "{} = ", attr.name)?;
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

#[derive(Debug)]
pub struct Insert<'a, T = Query<'a>>
{
    pub(crate) target: &'a str,
    pub(crate) contents: T,
}

impl<'a> Insert<'a, ()> {
    pub fn insert_into(target: &'a str) -> Self {
        Self { target, contents: () }
    }

    #[deprecated]
    pub fn tuple<T: Into<Query<'a>>>(self, tuple: T) -> Insert<'a>
    where T: IntoTuple<'a>,
    {
        Insert { contents: tuple.into(), target: self.target }
    }

    pub fn element(self, name: &'a str, value: impl Encode<'a>) -> Insert<'a, Vec<TupleAttr<'a>>> {
        let elem = TupleAttr { kind: None, name, value: value.encode() };
        Insert { target: self.target, contents: vec![elem] }
    }
}

impl<'a> Insert<'a, Vec<TupleAttr<'a>>> {
   pub fn element(mut self, name: &'a str, value: impl Encode<'a>) -> Self {
       let elem = TupleAttr { kind: None, name, value: value.encode() };
       self.contents.push(elem);
       self
    }
}

impl fmt::Display for Insert<'_, Vec<TupleAttr<'_>>> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{} ", self.target)?;
        write_attrs(f, &self.contents)?;
        Ok(())
    }
}

impl fmt::Display for Insert<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{} ", self.target)?;
        write!(f, "{}", &self.contents)?;
        Ok(())
    }
}

impl<'a> From<&'a [TupleAttr<'a>]> for Query<'a> {
    fn from(tuple: &'a [TupleAttr<'_>]) -> Self {
        Query::tuple(tuple)
    }
}

impl<'a> From<Vec<TupleAttr<'a>>> for Query<'a> {
    fn from(tuple: Vec<TupleAttr<'a>>) -> Self {
        Query::tuple(tuple)
    }
}

impl<'a> From<TupleBuilder<'a>> for Query<'a> {
    fn from(builder: TupleBuilder<'a>) -> Self {
        Query::from(builder.build())
    }
}

pub struct Delete<'a>(pub(crate) Query<'a>);

impl fmt::Display for Delete<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
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
        assert_eq!(query.to_string(), "scan example")
    }

    #[test]
    fn filter() {
        assert_eq!(
            Query::scan("example").filter("id", EQ, "1").select(&["id", "a_column"]).to_string(),
            "scan example | filter id = 1 | select id a_column");
        assert_eq!(
            Query::scan("example").filter("id", GT, "2").select_all().to_string(), "scan example | filter id > 2"
        );
    }

    #[test]
    fn source_is_tuple() {
        let query = Query::build_tuple().inferred("id", "1").inferred("value", "example_value").build().filter("id", Operator::EQ, "1");
        assert_eq!(query.to_string(), "tuple id = 1 value = example_value | filter id = 1");
    }

    #[test]
    fn join() {
        let query = Query::scan("example").join("example.type_id", "type.id");
        assert_eq!(query.to_string(), "scan example | join example.type_id type.id");
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
    fn map_set() {
        let query = Query::scan("example").set("example.name", "new name");
        assert_eq!(query.to_string(), "scan example | map set example.name \"new name\"");
    }

    #[test]
    fn index_scan() {
        let query = Query::scan_index("example.id", Operator::EQ, "1");
        assert_eq!(query.to_string(), "scan_index example.id = 1");
    }

    #[test]
    fn delete() {
        let delete_all = Query::scan("example").delete();
        assert_eq!(delete_all.to_string(), "scan example");

        let delete_one = Query::scan("example").filter("example.id", EQ, "1").delete();
        assert_eq!(delete_one.to_string(), "scan example | filter example.id = 1");
    }

    #[test]
    fn insert() {
        let insert = Insert::insert_into("example").element("id", 1).element("name", "something");
        assert_eq!("example id = 1 name = something", insert.to_string());
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
        let query = Query::tuple(&[("id", "1"), ("value", "foo bar")]).filter("id", Operator::EQ, "1");
        assert_eq!("tuple id = 1 value = \"foo bar\" | filter id = 1", query.to_string());
    }
}
