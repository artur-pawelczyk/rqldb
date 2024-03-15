use std::{fmt, str::FromStr};

use crate::{dsl::AttrKind, plan::Attribute};

#[derive(Default, PartialEq)]
pub struct Schema {
    pub relations: Vec<Relation>,
}

#[derive(Clone)]
pub struct Relation {
    pub id: usize,
    pub name: String,
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

impl TableId for usize {
    fn find_in(self, schema: &Schema) -> Option<&Relation> {
        schema.relations.iter().find(|rel| rel.id == self)
    }
}

impl TableId for &str {
    fn find_in(self, schema: &Schema) -> Option<&Relation> {
        schema.relations.iter().find(|rel| rel.name == self)
    }
}

impl TableId for &String {
    fn find_in(self, schema: &Schema) -> Option<&Relation> {
        schema.relations.iter().find(|rel| &rel.name == self)
    }
}

#[derive(Clone, PartialEq)]
pub struct Column<'a> {
    inner: &'a InnerColumn,
    table: &'a Relation,
    pos: usize,
}

impl<'a> fmt::Debug for Column<'a> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.name())
    }
}

impl Into<Attribute> for Column<'_> {
    fn into(self) -> Attribute {
        Attribute {
            pos: self.pos,
            name: Box::from(self.name()),
            kind: self.kind(),
        }
    }
}

impl<'a> Column<'a> {
    pub fn indexed(&self) -> bool {
        self.inner.indexed
    }

    pub fn pos(&self) -> usize {
        self.pos
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
}

#[derive(Clone, PartialEq)]
struct InnerColumn {
    name: String,
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

impl Into<AttrKind> for Type {
    fn into(self) -> AttrKind {
        match self {
            Self::NUMBER => AttrKind::Number,
            Self::TEXT => AttrKind::Text,
            _ => unimplemented!(),
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
    /// schema.create_table("example").indexed_column("id", Type::NUMBER).column("name", Type::TEXT).add();
    /// assert!(schema.find_relation("example").is_some());
    /// ```
    #[must_use]
    pub fn create_table(&mut self, name: &str) -> TableBuilder {
        TableBuilder { schema: self, name: name.to_string(), columns: vec![] }
    }

    pub fn find_relation<T: TableId>(&self, id: T) -> Option<&Relation> {
        id.find_in(self)
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
    pub fn find_column(&self, s: &str) -> Option<Column> {
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
            name: format!("{}.{}", self.name, name),
            kind,
            indexed: false
        });
        self
    }

    #[must_use]
    pub fn indexed_column(mut self, name: &str, kind: Type) -> Self {
        self.columns.push(InnerColumn {
            name: format!("{}.{}", self.name, name),
            kind,
            indexed: true
        });
        self
    }

    pub fn add(self) -> &'a Relation {
        let id = self.schema.relations.len();
        self.schema.relations.push(Relation { id, name: self.name, columns: self.columns });
        self.schema.relations.last().unwrap()
    }
}

pub struct ColumnIter<'a> {
    table: &'a Relation,
    pos: usize,
}

impl<'a> Iterator for ColumnIter<'a> {
    type Item = Column<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        let next = self.table.columns.get(self.pos).map(|inner| Column{ inner, table: self.table, pos: self.pos });
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

    pub fn attributes(&self) -> ColumnIter {
        ColumnIter{ table: self, pos: 0 }
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
    ///     .add();
    ///
    /// assert_eq!(relation.column_position("document.id"), Some(0));
    /// assert_eq!(relation.column_position("something.id"), None);
    /// ```
    pub fn column_position(&self, name: &str) -> Option<u32> {
        if let Some((pos, _)) = self.columns.iter().enumerate().find(|(_, col)| col.name == name) {
            Some(pos as u32)
        } else {
            None
        }
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
    ///     .add();
    ///
    /// assert_eq!(relation.column_type("document.id"), Some(Type::NUMBER));
    /// assert_eq!(relation.column_type("something.id"), None);
    /// ```
    pub fn column_type(&self, name: &str) -> Option<Type> {
        self.columns.iter().find(|col| col.name == name).map(|col| col.kind)
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
    pub fn indexed_column(&self) -> Option<Column> {
        self.columns.iter().enumerate().find(|(_, col)| col.indexed).map(|(pos, col)| Column{ table: self, inner: col, pos })
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
    pub fn find_column(&self, name: &str) -> Option<Column> {
        self.columns.iter().enumerate().find(|(_, col)| col.name == name).map(|(pos, col)| Column{ table: self, inner: col, pos })
    }

    pub fn types(&self) -> Vec<Type> {
        self.columns.iter().map(|col| col.kind).collect()
    }
}

