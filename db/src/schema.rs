#[derive(Default)]
pub struct Schema {
    pub relations: Vec<Relation>
}

pub struct Relation {
    pub name: String,
    pub columns: Vec<Column>
}

#[derive(Clone)]
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
    pub fn add_relation(&mut self, name: &str, columns: &[Column]) {
        let relation = Relation{name: name.to_string(), columns: columns.to_vec()};
        self.relations.push(relation);
    }

    pub fn find_relation(&self, name: &str) -> Option<&Relation> {
        self.relations.iter()
            .find(|x| x.name == name)
    }
}

impl Relation {
    
    /// # Examples
    /// 
    /// ```
    /// use rqldb::schema::{Column, Type, Relation};
    ///
    /// let relation = Relation{name: "document".to_string(),
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
    /// let relation = Relation{name: "document".to_string(),
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

