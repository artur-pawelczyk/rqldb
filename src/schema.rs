pub struct Schema {
    relations: Vec<Relation>
}

struct Relation {
    name: String,
    columns: Vec<Column>
}

pub struct Column {
    pub name: String,
    pub kind: Type
}

pub enum Type {
    NUMBER, TEXT
}

impl Schema {
    pub fn new() -> Self{
        Self{relations: Vec::new()}
    }
}
