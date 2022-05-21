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

#[derive(Clone)]
pub enum Type {
    NUMBER, TEXT
}

impl Schema {
    pub fn new() -> Self{
        Self{relations: Vec::new()}
    }

    pub fn add_relation(&mut self, name: &str, columns: &[Column]) {
        let relation = Relation{name: name.to_string(), columns: columns.to_vec()};
        self.relations.push(relation);
    }
}
