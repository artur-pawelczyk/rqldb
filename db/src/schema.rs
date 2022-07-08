#[derive(Default)]
pub struct Schema {
    pub relations: Vec<Relation>,
}

#[derive(Debug)]
pub struct Relation {
    pub id: usize,
    pub name: String,
    pub columns: Vec<Column>
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

#[derive(Clone, Debug)]
pub struct Column {
    pub name: String,
    pub kind: Type
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

impl Schema {
    pub fn add_relation(&mut self, id: usize, name: &str, columns: &[Column]) {
        let relation = Relation{ id, name: name.to_string(), columns: columns.to_vec() };
        self.relations.push(relation);
    }

    pub fn find_relation<T: TableId>(&self, id: T) -> Option<&Relation> {
        id.find_in(self)
    }
}

pub struct ColumnIter<'a> {
    name: &'a str,
    raw: &'a [Column],
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

    pub fn columns<'a>(&'a self) -> ColumnIter<'a> {
        ColumnIter{ name: self.name.as_str(), raw: &self.columns, pos: 0 }
    }
    
    /// # Examples
    /// 
    /// ```
    /// use rqldb::schema::{Column, Type, Relation};
    ///
    /// let relation = Relation{id: 0, name: "document".to_string(),
    ///                         columns: vec![
    ///                             Column{name: "id".to_string(), kind: Type::NUMBER},
    ///                             Column{name: "content".to_string(), kind: Type::TEXT},
    ///                         ]};
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
    /// use rqldb::schema::{Column, Type, Relation};
    ///
    /// let relation = Relation{id: 0, name: "document".to_string(),
    ///                         columns: vec![
    ///                             Column{name: "id".to_string(), kind: Type::NUMBER},
    ///                             Column{name: "content".to_string(), kind: Type::TEXT},
    ///                         ]};
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

    pub fn full_attribute_names(&self) -> Vec<String> {
        self.columns.iter().map(|col| format!("{}.{}", self.name, col.name)).collect()
    }

    pub fn types(&self) -> Vec<Type> {
        self.columns.iter().map(|col| col.kind).collect()
    }

    pub fn attributes(&self) -> Vec<(String, Type)> {
        self.columns.iter().map(|col| (format!("{}.{}", self.name, col.name), col.kind)).collect()
    }

    fn column_name_matches(&self, col: &Column, name: &str) -> bool {
        let parts: Vec<&str> = name.split('.').collect();
        match parts[..] {
            [first] => col.name == first,
            [first, second] => first == self.name && col.name == second,
            _ => false
        }
    }
}

