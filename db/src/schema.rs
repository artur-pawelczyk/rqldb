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
    NUMBER, TEXT
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
    pub fn column_position(&self, name: &str) -> Option<u32> {
        if let Some((pos, _)) = self.columns.iter().enumerate().find(|(_, col)| col.name == name) {
            Some(pos as u32)
        } else {
            None
        }
    }
}
