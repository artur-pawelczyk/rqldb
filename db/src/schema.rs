use std::{fmt, str::FromStr};

#[derive(Default, PartialEq)]
pub struct Schema {
    pub relations: Vec<Relation>,
}

#[derive(Debug)]
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

#[derive(Clone, Debug, PartialEq)]
pub struct Column<'a> {
    inner: &'a InnerColumn,
    pos: usize,
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

    pub fn kind(&self) -> Type {
        self.inner.kind
    }
}

#[derive(Debug, PartialEq)]
struct InnerColumn {
    name: String,
    kind: Type,
    indexed: bool,
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub enum Type {
    BYTE(u8),
    NUMBER, TEXT,
    NONE
}

impl Default for Type {
    fn default() -> Self {
        Self::NONE
    }
}

impl fmt::Display for Type {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let s = match self {
            Type::NUMBER => "NUMBER",
            Type::TEXT => "TEXT",
            Type::BYTE(n) => if n == &1 { "UINT8" } else if n == &2 { "UINT16" } else if n == &4 { "UINT32" } else { panic!() },
            Type::NONE => panic!(),
        };

        write!(f, "{}", s)
    }
}

impl FromStr for Type {
    type Err = ();

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "NUMBER" => Ok(Type::NUMBER),
            "TEXT" => Ok(Type::TEXT),
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
}

pub struct TableBuilder<'a> {
    schema: &'a mut Schema,
    name: String,
    columns: Vec<InnerColumn>,
}

impl<'a> TableBuilder<'a> {
    #[must_use]
    pub fn column(mut self, name: &str, kind: Type) -> Self {
        self.columns.push(InnerColumn{ name: name.to_string(), kind, indexed: false });
        self
    }

    #[must_use]
    pub fn indexed_column(mut self, name: &str, kind: Type) -> Self {
        self.columns.push(InnerColumn{ name: name.to_string(), kind, indexed: true });
        self
    }

    pub fn add(self) -> &'a Relation {
        let id = self.schema.relations.len();
        self.schema.relations.push(Relation { id, name: self.name, columns: self.columns });
        self.schema.relations.last().unwrap()
    }
}

pub struct ColumnIter<'a> {
    name: &'a str,
    raw: &'a [InnerColumn],
    pos: usize,
}

impl<'a> Iterator for ColumnIter<'a> {
    type Item = (String, Type);

    fn next(&mut self) -> Option<(String, Type)> {
        self.pos += 1;
        self.raw.get(self.pos - 1)
            .map(|col| (format!("{}.{}", self.name, col.name), col.kind))
    }
}

impl Relation {

    pub fn columns(&self) -> ColumnIter {
        ColumnIter{ name: self.name.as_str(), raw: &self.columns, pos: 0 }
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
    /// assert_eq!(relation.column_position("id"), Some(0));
    /// assert_eq!(relation.column_position("content"), Some(1));
    /// assert_eq!(relation.column_position("nothing"), None);
    ///
    /// assert_eq!(relation.column_position("document.id"), Some(0));
    /// assert_eq!(relation.column_position("something.id"), None);
    /// ```
    pub fn column_position(&self, name: &str) -> Option<u32> {
        if let Some((pos, _)) = self.columns.iter().enumerate().find(|(_, col)| self.column_name_matches(col, name)) {
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
    /// assert_eq!(relation.column_type("id"), Some(Type::NUMBER));
    /// assert_eq!(relation.column_type("content"), Some(Type::TEXT));
    /// assert_eq!(relation.column_type("nothing"), None);
    ///
    /// assert_eq!(relation.column_type("document.id"), Some(Type::NUMBER));
    /// assert_eq!(relation.column_type("something.id"), None);
    /// ```
    pub fn column_type(&self, name: &str) -> Option<Type> {
        self.columns.iter().find(|col| self.column_name_matches(col, name)).map(|col| col.kind)
    }

    /// # Examples
    ///
    /// ```
    /// use rqldb::schema::Schema;
    /// use rqldb::Type;
    /// 
    /// let mut schema = Schema::default();
    /// schema.create_table("example").indexed_column("id", Type::NUMBER).column("name", Type::TEXT).add();
    /// assert_eq!(schema.find_relation("example").unwrap().indexed_column().unwrap().name(), "id");
    /// ```
    pub fn indexed_column(&self) -> Option<Column> {
        self.columns.iter().enumerate().find(|(_, col)| col.indexed).map(|(pos, col)| Column{ inner: col, pos })
    }

    pub fn full_attribute_names(&self) -> Vec<String> {
        self.columns.iter().map(|col| format!("{}.{}", self.name, col.name)).collect()
    }

    pub fn types(&self) -> Vec<Type> {
        self.columns.iter().map(|col| col.kind).collect()
    }

    pub fn attributes(&self) -> Vec<(String, Type)> {
        self.columns.iter().map(|col| (format!("{}.{}", self.name, col.name), col.kind)).collect()
    }

    fn column_name_matches(&self, col: &InnerColumn, name: &str) -> bool {
        let parts: Vec<&str> = name.split('.').collect();
        match parts[..] {
            [first] => col.name == first,
            [first, second] => first == self.name && col.name == second,
            _ => false
        }
    }
}

