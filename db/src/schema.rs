use std::{fmt, str::FromStr};

use crate::{object::Attribute, tuple::PositionalAttribute};

#[derive(Default, PartialEq)]
pub struct Schema {
    pub relations: Vec<Relation>,
}

#[derive(Clone)]
pub struct Relation {
    pub id: u32,
    pub name: Box<str>,
    columns: Vec<InnerColumn>,
}

impl PartialEq for Relation {
    fn eq(&self, other: &Self) -> bool {
        self.name == other.name
    }
}

impl fmt::Debug for Relation {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Relation {}; {}", self.id, self.name)
    }
}

pub trait TableId {
    fn find_in(self, schema: &Schema) -> Option<&Relation>;
}

impl TableId for u32 {
    fn find_in(self, schema: &Schema) -> Option<&Relation> {
        schema.relations.iter().find(|rel| rel.id == self)
    }
}

impl TableId for &str {
    fn find_in(self, schema: &Schema) -> Option<&Relation> {
        let rel_name = self.find('.').map(|i| &self[..i]).unwrap_or(self);
        schema.relations.iter().find(|rel| rel.name.as_ref() == rel_name)
    }
}

impl TableId for &String {
    fn find_in(self, schema: &Schema) -> Option<&Relation> {
        schema.relations.iter().find(|rel| rel.name.as_ref() == self)
    }
}

impl TableId for &AttributeRef {
    fn find_in(self, schema: &Schema) -> Option<&Relation> {
        schema.relations.get(self.rel_id? as usize)
    }
}

impl TableId for &Column<'_> {
    fn find_in(self, schema: &Schema) -> Option<&Relation> {
        self.reference().rel_id.and_then(|id| schema.relations.get(id as usize))
    }
}

pub trait AttributeIdentifier {
    fn find_in_schema(self, schema: &Schema) -> Option<Column<'_>>;
    fn find_in_relation(self, rel: &Relation) -> Option<Column<'_>>;
}

impl AttributeIdentifier for &str {
    fn find_in_schema(self, schema: &Schema) -> Option<Column<'_>> {
        schema.find_column(self)
    }

    fn find_in_relation(self, rel: &Relation) -> Option<Column<'_>> {
        let (rel_name, _) = split_name(self)?;
        if rel_name == rel.name.as_ref() {
            rel.find_column(self)
        } else {
            None
        }
    }
}

impl AttributeIdentifier for &AttributeRef {
    fn find_in_schema(self, schema: &Schema) -> Option<Column<'_>> {
        let rel = schema.relations.get(self.rel_id? as usize)?;
        rel.attribute_by_id(self.attr_id)
    }

    fn find_in_relation(self, rel: &Relation) -> Option<Column<'_>> {
        if self.rel_id? == rel.id {
            rel.attribute_by_id(self.attr_id)
        } else {
            None
        }
    }
}

#[derive(Debug, Hash, PartialEq, Eq, PartialOrd, Ord, Clone, Copy)]
pub struct AttributeRef {
    pub(crate) rel_id: Option<u32>,
    pub(crate) attr_id: u32,
}

impl AttributeRef {
    pub(crate) fn temporary(attr_id: u32) -> Self {
        Self { attr_id, rel_id: None }
    }
}

impl PositionalAttribute for AttributeRef {
    fn pos(&self) -> u32 {
        self.attr_id
    }

    fn object_id(&self) -> Option<u32> {
        self.rel_id
    }
}

#[derive(Clone, PartialEq)]
pub struct Column<'a> {
    inner: &'a InnerColumn,
    table: &'a Relation,
    id: u32,
    rel_id: u32,
}

impl<'a> fmt::Debug for Column<'a> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.name())
    }
}

impl From<Column<'_>> for Attribute {
    fn from(a: Column<'_>) -> Self {
        Self {
            name: Box::from(a.name()),
            kind: a.kind(),
            reference: a.reference(),
        }
    }
}

impl From<&Column<'_>> for Attribute {
    fn from(a: &Column<'_>) -> Self {
        Self {
            name: Box::from(a.name()),
            kind: a.kind(),
            reference: a.reference(),
        }
    }
}

impl<'a> Column<'a> {
    pub fn indexed(&self) -> bool {
        self.inner.indexed
    }

    pub fn name(&self) -> &str {
        &self.inner.name
    }

    pub fn short_name(&self) -> &str {
        self.inner.name.find('.')
            .map(|i| &self.inner.name[i+1..])
            .unwrap_or(&self.inner.name)
    }

    pub fn kind(&self) -> Type {
        self.inner.kind
    }

    pub fn table(&self) -> &Relation {
        self.table
    }

    pub fn reference(&self) -> AttributeRef {
        AttributeRef {
            rel_id: Some(self.rel_id),
            attr_id: self.id
        }
    }

    pub fn belongs_to(&self, rel: &Relation) -> bool {
        self.table == rel
    }
}

#[derive(Clone, PartialEq)]
struct InnerColumn {
    name: Box<str>,
    kind: Type,
    indexed: bool,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Type {
    BYTE(u8),
    NUMBER, TEXT, BOOLEAN,
    NONE
}

impl Default for Type {
    fn default() -> Self {
        Self::NONE
    }
}

impl fmt::Display for Type {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Type::NUMBER => write!(f, "NUMBER"),
            Type::TEXT => write!(f, "TEXT"),
            Type::BOOLEAN => write!(f, "BOOLEAN"),
            Type::BYTE(n) => if *n > 0 && *n < 4 { write!(f, "UINT{}", n*8) } else { Err(fmt::Error) },
            Type::NONE => write!(f, "NONE"),
        }
    }
}

impl FromStr for Type {
    type Err = ();

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "NUMBER" => Ok(Type::NUMBER),
            "TEXT" => Ok(Type::TEXT),
            "BOOLEAN" => Ok(Type::BOOLEAN),
            "UINT8" => Ok(Type::BYTE(8)),
            "UINT16" => Ok(Type::BYTE(16)),
            "UINT32" => Ok(Type::BYTE(32)),
            _ => Err(()),
        }
    }
}

impl Schema {
    /// # Examples
    ///
    /// ```
    /// use rqldb::schema::Schema;
    /// use rqldb::Type;
    /// 
    /// let mut schema = Schema::default();
    /// let relation = schema.create_table("example").indexed_column("id", Type::NUMBER).column("name", Type::TEXT).add();
    /// assert!(relation.is_some());
    /// assert!(schema.find_relation("example").is_some());
    ///
    /// // Trying to create a new relation with the same name
    /// assert!(schema.create_table("example").column("id", Type::NUMBER).add().is_none());
    /// ```
    #[must_use]
    pub fn create_table(&mut self, name: &str) -> TableBuilder<'_> {
        TableBuilder { schema: self, name: name.to_string(), columns: vec![] }
    }

    pub fn find_relation<T: TableId>(&self, id: T) -> Option<&Relation> {
        id.find_in(self)
    }

    pub fn lookup_attribute(&self, id: impl AttributeIdentifier) -> Option<Column<'_>> {
        id.find_in_schema(self)
    }

    /// # Examples
    ///
    /// ```
    /// use rqldb::schema::Schema;
    /// use rqldb::Type;
    /// 
    /// let mut schema = Schema::default();
    /// schema.create_table("example").indexed_column("id", Type::NUMBER).column("name", Type::TEXT).add();
    /// assert_eq!(schema.find_column("example.id").unwrap().name(), "example.id");
    /// ```
    pub fn find_column(&self, s: &str) -> Option<Column<'_>> {
        let rel_name = s.split('.').next()?;
        self.find_relation(rel_name).and_then(|rel| rel.find_column(s))
    }
}

pub struct TableBuilder<'a> {
    schema: &'a mut Schema,
    name: String,
    columns: Vec<InnerColumn>,
}

impl<'a> TableBuilder<'a> {
    #[must_use]
    pub fn column(mut self, name: &str, kind: Type) -> Self {
        self.columns.push(InnerColumn {
            name: Box::from(format!("{}.{}", self.name, name)),
            kind,
            indexed: false
        });
        self
    }

    #[must_use]
    pub fn indexed_column(mut self, name: &str, kind: Type) -> Self {
        self.columns.push(InnerColumn {
            name: Box::from(format!("{}.{}", self.name, name)),
            kind,
            indexed: true
        });
        self
    }

    pub fn add(self) -> Option<&'a Relation> {
        if self.schema.find_relation(&self.name).is_some() {
            return None;
        }

        let id = self.schema.relations.len() as u32;
        self.schema.relations.push(Relation { id, name: Box::from(self.name), columns: self.columns });
        self.schema.relations.last()
    }
}

pub struct ColumnIter<'a> {
    table: &'a Relation,
    rel_id: u32,
    pos: usize,
}

impl<'a> Iterator for ColumnIter<'a> {
    type Item = Column<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        let next = self.table.columns.get(self.pos).map(|inner| Column {
            inner,
            table: self.table,
            id: self.pos as u32,
            rel_id: self.rel_id
        });
        self.pos += 1;
        next
    }
}

impl ExactSizeIterator for ColumnIter<'_> {
    fn len(&self) -> usize {
        self.table.columns.len() - self.pos
    }
}

impl Relation {

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn attributes(&self) -> ColumnIter<'_> {
        ColumnIter { table: self, pos: 0, rel_id: self.id }
    }


    /// # Examples
    /// 
    /// ```
    /// use rqldb::schema::{Column, Type, Relation, Schema};
    ///
    /// let mut schema = Schema::default();
    /// let relation = schema.create_table("document")
    ///     .column("id", Type::NUMBER)
    ///     .column("content", Type::TEXT)
    ///     .add().unwrap();
    ///
    /// assert_eq!(relation.column_type("document.id"), Some(Type::NUMBER));
    /// assert_eq!(relation.column_type("something.id"), None);
    /// ```
    pub fn column_type(&self, name: &str) -> Option<Type> {
        self.columns.iter().find(|col| col.name.as_ref() == name).map(|col| col.kind)
    }

    /// # Examples
    ///
    /// ```
    /// use rqldb::schema::Schema;
    /// use rqldb::Type;
    /// 
    /// let mut schema = Schema::default();
    /// schema.create_table("example").indexed_column("id", Type::NUMBER).column("name", Type::TEXT).add();
    /// assert_eq!(schema.find_relation("example").unwrap().indexed_column().unwrap().short_name(), "id");
    /// ```
    pub fn indexed_column(&self) -> Option<Column<'_>> {
        self.columns.iter().enumerate().find(|(_, col)| col.indexed).map(|(pos, col)| Column {
            table: self,
            inner: col,
            id: pos as u32,
            rel_id: self.id
        })
    }

    /// # Examples
    ///
    /// ```
    /// use rqldb::schema::Schema;
    /// use rqldb::Type;
    /// 
    /// let mut schema = Schema::default();
    /// schema.create_table("example").indexed_column("id", Type::NUMBER).column("name", Type::TEXT).add();
    /// assert_eq!(schema.find_relation("example").unwrap().find_column("example.name").unwrap().short_name(), "name");
    /// ```
    pub fn find_column(&self, name: &str) -> Option<Column<'_>> {
        self.columns.iter().enumerate()
            .find(|(_, col)| col.name.as_ref() == name)
            .map(|(pos, col)| Column {
                table: self,
                inner: col,
                id: pos as u32,
                rel_id: self.id,
            })
    }

    fn attribute_by_id(&self, attr_id: u32) -> Option<Column<'_>> {
        let rel = self.columns.get(attr_id as usize)?;
        Some(Column {
            table: self,
            inner: rel,
            id: attr_id,
            rel_id: self.id,
        })
    }

    pub fn lookup(&self, attr: impl AttributeIdentifier) -> Option<Column<'_>> {
        attr.find_in_relation(self)
    }

    pub fn types(&self) -> Vec<Type> {
        self.columns.iter().map(|col| col.kind).collect()
    }
}

fn split_name(name: &str) -> Option<(&str, &str)> {
    name.find('.').map(|i| (&name[..i], &name[i+1..]))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_split_name() {
        assert_eq!(split_name("document.id"), Some(("document", "id")));
        assert_eq!(split_name("document"), None);
        assert_eq!(split_name("id"), None);
    }
}
