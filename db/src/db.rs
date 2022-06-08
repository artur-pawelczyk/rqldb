use std::collections::HashMap;
use std::iter::zip;

use crate::select::{SelectQuery, Source, Finisher, Operator, Filter, JoinSource};
use crate::schema::{Column, Schema, Type};
use crate::create::CreateRelationCommand;
use crate::{Cell, QueryResults};

#[derive(Default)]
pub struct Database {
    schema: Schema,
    objects: HashMap<String, Object>
}

type Object = Vec<ByteTuple>;
type ByteTuple = Vec<Vec<u8>>;

struct TupleSet(Vec<Attribute>, Vec<Tuple>);
struct TupleView<'a> {
    attributes: &'a [Attribute],
    raw: &'a Tuple,
}
struct TupleIter<'a> {
    tuple_set: &'a TupleSet,
    pos: usize,
}

#[derive(Clone, Debug)]
pub enum Attribute {
    Unnamed(Type, i32),
    Named(Type, String),
    Absolute(Type, String, String),
}

#[derive(Clone, Debug)]
pub struct Tuple {
    attributes: Vec<Attribute>,
    contents: Vec<Cell>,
}

impl Tuple {
    fn from_bytes(attrs: &[Attribute], bytes: &[Vec<u8>]) -> Tuple {
        let cells: Vec<Cell> = zip(attrs, bytes).map(|(attr, b)| Cell::from_bytes(attr.kind(), b)).collect();
        Self{
            attributes: attrs.iter().map(Attribute::clone).collect(),
            contents: cells
        }
    }

    fn from_cells(cells: Vec<Cell>) -> Self {
        let attrs: Vec<Attribute> = cells.iter().enumerate().map(|(i, c)| Attribute::Unnamed(c.kind, i as i32)).collect();
        Self{ attributes: attrs, contents: cells }
    }

    pub fn len(&self) -> usize {
        self.contents.len()
    }

    pub fn cell_at(&self, i: u32) -> Option<&Cell> {
        self.contents.get(i as usize)
    }

    pub fn cell_by_name(&self, name: &str) -> Option<&Cell> {
        if let Some((idx, _)) = self.attributes.iter().enumerate().find(|(_, attr)| attr.as_string() == name) {
            self.cell_at(idx as u32)
        } else {
            None
        }
    }

    pub fn contents(&self) -> &[Cell] {
        &self.contents
    }

    pub fn into_cells(self) -> Vec<Cell> {
        self.contents
    }

    fn add_cells(mut self, cells: &[Cell]) -> Self {
        for c in cells {
            self.contents.push(c.clone());
        }

        self
    }
}



impl Database {
    pub fn execute_create(&mut self, command: &CreateRelationCommand) {
        self.schema.add_relation(&command.name, &command.columns);
        self.objects.insert(command.name.clone(), Object::new());
    }

    pub fn execute_query(&self, query: &SelectQuery) -> Result<QueryResults, &str> {
        let source_tuples = read_source(self, &query.source)?;
        let joined_tuples = execute_join(self, source_tuples, &query.join_sources)?;
        let filtered_tuples = filter_tuples(joined_tuples, &query.filters);

        match query.finisher {
            Finisher::AllColumns => Result::Ok(filtered_tuples.into_query_results()),
            Finisher::Columns(_) => todo!(),
            Finisher::Insert(_) => Result::Err("Can't run a mutating query"),
            Finisher::Count => Ok(QueryResults::count(filtered_tuples.count())),
        }
    }

    pub fn execute_mut_query(&mut self, query: &SelectQuery) -> Result<QueryResults, &str> {
        let source_tuples = read_source(self, &query.source)?;
        let joined_tuples = execute_join(self, source_tuples, &query.join_sources)?;
        let filtered_tuples = filter_tuples(joined_tuples, &query.filters);

        match &query.finisher {
            Finisher::Insert(table) => {
                let table_schema = self.schema.find_relation(table).ok_or("No such table")?;
                let object = self.objects.entry(table.to_string()).or_insert(Object::new());
                for tuple in filtered_tuples.contents() {
                    if !validate_with_schema(&table_schema.columns, tuple) { return Err("Invalid input") }
                    object.push(tuple.contents.iter().map(|x| x.as_bytes()).collect())
                }

                Result::Ok(filtered_tuples.into_query_results())
            },
            Finisher::AllColumns => Result::Ok(filtered_tuples.into_query_results()),
            Finisher::Columns(_) => todo!(),
            Finisher::Count => Ok(QueryResults::count(filtered_tuples.count())),
        }
    }

    fn scan_table(&self, name: &str) -> Result<TupleSet, &'static str> {
        let rel = self.schema.find_relation(name).ok_or("No such relation in schema")?;
        let attributes: Vec<Attribute> = rel.columns.iter()
            .map(|col| (col.kind, col.name.clone()))
            .map(|(col_kind, col_name)| Attribute::Absolute(col_kind, name.to_string(), col_name)).collect();
        let values = self.objects.get(name).ok_or("Could not find the object")?;
        let mut tuple_set = TupleSet::with_attributes(attributes);
        for val in values {
            tuple_set.add_tuple(val);
        }

        Ok(tuple_set)
    }
}

fn validate_with_schema(columns: &[Column], tuple: &Tuple) -> bool {
    if columns.len() == tuple.len() {
        zip(columns, &tuple.contents).all(|(col, cell)| col.kind == cell.kind)
    } else {
        false
    }
}

fn read_source(db: &Database, source: &Source) -> Result<TupleSet, &'static str> {
    return match &source {
        Source::TableScan(name) => {
            db.scan_table(name)
        },
        Source::Tuple(values) => {
            let cells = values.iter().map(|x| Cell::from_string(x)).collect();
            Ok(TupleSet::single_from_cells(cells))
        }
    }
}

fn execute_join(db: &Database, current_tuples: TupleSet, joins: &[JoinSource]) -> Result<TupleSet, &'static str> {
    match joins {
        [] => Ok(current_tuples),
        [join] => {
            let joiner = db.scan_table(&join.table)?;
            Ok(current_tuples.join(joiner, (&join.left, &join.right)))
        }
        _ => todo!(),
    }
}

fn join_with_matched_tuple(joinee: Tuple, joiner: &TupleSet, (left, right): (&str, &str)) -> Option<Tuple> {
    let key = joinee.cell_by_name(&left)?;
    let tuple = joiner.iter().find(|t| t.cell_by_name(&right).expect("Expected a cell") == key)?;
    Some(joinee.add_cells(&tuple.contents()))
}

fn filter_tuples(source: TupleSet, filters: &[Filter]) -> TupleSet {
    match filters {
        [] => source,
        filters => {
            let mut tuples = source.1;
            for filter in filters {
                tuples = apply_filter(tuples, filter);
            }

            TupleSet(source.0, tuples)
        },
    }
}

fn apply_filter(source: Vec<Tuple>, filter: &Filter) -> Vec<Tuple> {
    source.into_iter()
        .filter(|tuple| test_filter(filter, tuple))
        .collect()
}

fn test_filter(filter: &Filter, tuple: &Tuple) -> bool {
    match filter {
        Filter::Condition(left, op, right) => {
            let cell = tuple.cell_by_name(left).unwrap();
            let right_as_cell = Cell::from_string(right);
            match op {
                Operator::EQ => cell == &right_as_cell,
                Operator::GT => cell > &right_as_cell,
                Operator::GE => cell >= &right_as_cell,
                Operator::LT => cell < &right_as_cell,
                Operator::LE => cell <= &right_as_cell,
            }
        }
    }
}

/// If all the attributes are in the same table, make them all non-absolute
fn simplify_attributes(attrs: Vec<Attribute>) -> Vec<Attribute> {
    let mut tables: Vec<&str> = attrs.iter().flat_map(|attr| attr.table()).collect();
    tables.dedup();

    if tables.len() == 1 {
        attrs.into_iter().map(|attr| attr.into_named()).collect()
    } else {
        attrs
    }
}

impl TupleSet {
    fn with_attributes(attributes: Vec<Attribute>) -> Self {
        Self(attributes, vec![])
    }

    fn single_from_cells(cells: Vec<Cell>) -> Self {
        let attrs: Vec<Attribute> = cells.iter().enumerate().map(|(i, c)| Attribute::Unnamed(c.kind, i as i32)).collect();
        Self(attrs, vec![Tuple::from_cells(cells)])
    }

    fn add_tuple(&mut self, values: &[Vec<u8>]) {
        self.1.push(Tuple::from_bytes(&self.0, values));
    }

    fn join(mut self, mut other: TupleSet, join_spec: (&str, &str)) -> Self {
        self.1 = self.1.into_iter().flat_map(|t| join_with_matched_tuple(t, &other, join_spec)).collect();
        for attr in other.take_attributes() {
            self.0.push(attr);
        }
        self
    }

    fn take_attributes(&mut self) -> Vec<Attribute> {
        std::mem::take(&mut self.0)
    }

    #[deprecated]
    fn contents(&self) -> &[Tuple] {
        &self.1
    }

    fn iter(&self) -> TupleIter {
        TupleIter{tuple_set: &self, pos: 0}
    }

    fn count(&self) -> i32 {
        self.1.len() as i32
    }

    fn into_query_results(self) -> QueryResults {
        QueryResults{
            attributes: simplify_attributes(self.0).iter().map(|x| x.as_string().to_string()).collect(),
            results: self.1.into_iter().map(Tuple::into_cells).collect()
        }
    }
}

impl<'a> Iterator for TupleIter<'a> {
    type Item = TupleView<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        let raw_tuple = self.tuple_set.1.get(self.pos);
        self.pos = self.pos + 1;
        raw_tuple.map(|t| TupleView{attributes: &self.tuple_set.0, raw: t})
    }
}

impl<'a> TupleView<'a> {
    pub fn cell_by_name(&self, name: &str) -> Option<&Cell> {
        if let Some((idx, _)) = self.attributes.iter().enumerate().find(|(_, attr)| attr.as_string() == name) {
            self.raw.cell_at(idx as u32)
        } else {
            None
        }
    }

    pub fn contents(&self) -> &[Cell] {
        &self.raw.contents
    }
}

impl Attribute {
    fn as_string(&self) -> String {
        match self {
            Attribute::Unnamed(_, i) => i.to_string(),
            Attribute::Named(_, s) => s.to_string(),
            Attribute::Absolute(_, t, s) => format!("{}.{}", t, s),
        }
    }

    fn kind(&self) -> Type {
        match self {
            Attribute::Absolute(k, _, _) => *k,
            Attribute::Unnamed(k, _) => *k,
            Attribute::Named(k, _) => *k,
        }
    }

    fn table<'a>(&'a self) -> Option<&'a str> {
        match self {
            Attribute::Absolute(_, t, _) => Some(t),
            _ => None,
        }
    }

    fn into_named(self) -> Attribute {
        match self {
            Attribute::Absolute(k, _, n) => Attribute::Named(k, n),
            Attribute::Named(k, n) => Attribute::Named(k, n),
            Attribute::Unnamed(k, i) => Attribute::Named(k, i.to_string()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn query_not_existing_relation() {
        let query = SelectQuery::scan("not_real_relation").select_all();
        let db = Database::default();
        let result = db.execute_query(&query);
        assert!(!result.is_ok());
    }

    #[test]
    fn query_empty_relation() {
        let mut db = Database::default();
        let command = CreateRelationCommand::with_name("document")
            .column("id", Type::NUMBER)
            .column("content", Type::TEXT);

        db.execute_create(&command);

        let query = SelectQuery::scan("document").select_all();
        let result = db.execute_query(&query);

        assert!(result.is_ok());
        let tuples = result.unwrap();
        assert_eq!(tuples.size(), 0);
        let attrs = tuples.attributes();
        assert_eq!(attrs.as_slice(), ["id".to_string(), "content".to_string()]);
    }

    #[test]
    pub fn insert() {
        let mut db = Database::default();

        let command = CreateRelationCommand::with_name("document")
            .column("id", Type::NUMBER)
            .column("content", Type::TEXT);
        db.execute_create(&command);

        let insert_query = SelectQuery::tuple(&["1".to_string(), "something".to_string()]).insert_into("document");
        let insert_result = db.execute_mut_query(&insert_query);
        assert!(insert_result.is_ok());

        let query = SelectQuery::scan("document").select_all();
        let result = db.execute_query(&query);
        assert!(result.is_ok());
        let tuples = result.unwrap();
        assert_eq!(tuples.size(), 1);
        let results = tuples.results();
        let tuple = results.iter().next().expect("fail");
        assert_eq!(&tuple.contents[0].as_bytes(), &Vec::from(1_i32.to_be_bytes()));
        assert_eq!(&tuple.contents[1].as_string(), "something");
    }

    #[test]
    pub fn failed_insert() {
        let mut db = Database::default();

        let command = CreateRelationCommand::with_name("document")
            .column("id", Type::NUMBER)
            .column("content", Type::TEXT);
        db.execute_create(&command);

        let result = db.execute_mut_query(&SelectQuery::tuple(&["not-a-number".to_string(), "random-text".to_string()]).insert_into("document"));
        assert!(result.is_err());
    }

    #[test]
    pub fn filter() {
        let mut db = Database::default();
        db.execute_create(&CreateRelationCommand::with_name("document").column("id", Type::NUMBER).column("content", Type::TEXT));

        for i in 1..20 {
            let content = format!("example{}", i);
            db.execute_mut_query(&SelectQuery::tuple(&[i.to_string(), content]).insert_into("document")).expect("Insert");
        }

        let mut result = db.execute_query(&SelectQuery::scan("document").filter("document.id", Operator::EQ, "5")).unwrap();
        assert_eq!(result.size(), 1);

        result = db.execute_query(&SelectQuery::scan("document").filter("document.id", Operator::GT, "5").filter("document.id", Operator::LT, "10")).unwrap();
        assert_eq!(result.size(), 4);

        result = db.execute_query(&SelectQuery::scan("document").filter("document.content", Operator::EQ, "example1")).unwrap();

        assert_eq!(result.size(), 1);
    }

    #[test]
    pub fn join() {
        let mut db = Database::default();
        db.execute_create(&CreateRelationCommand::with_name("document").column("id", Type::NUMBER).column("content", Type::TEXT).column("type_id", Type::NUMBER));
        db.execute_create(&CreateRelationCommand::with_name("type").column("id", Type::NUMBER).column("name", Type::TEXT));

        db.execute_mut_query(&SelectQuery::tuple(&["1", "example", "2"]).insert_into("document")).unwrap();
        db.execute_mut_query(&SelectQuery::tuple(&["1", "type_a"]).insert_into("type")).unwrap();
        db.execute_mut_query(&SelectQuery::tuple(&["2", "type_b"]).insert_into("type")).unwrap();

        let result = db.execute_query(&SelectQuery::scan("document").join("type", "document.type_id", "type.id")).unwrap();
        assert_eq!(*result.attributes, ["document.id", "document.content", "document.type_id", "type.id", "type.name"]);
        let tuple = result.tuple_at(0).unwrap();
        let document_id = tuple.cell_by_name("document.id").unwrap().as_string();
        let type_name = tuple.cell_by_name("type.name").unwrap().as_string();
        assert_eq!(document_id, "1");
        assert_eq!(type_name, "type_b");
    }

    #[test]
    pub fn count() {
        let mut db = Database::default();
        db.execute_create(&CreateRelationCommand::with_name("document").column("id", Type::NUMBER).column("content", Type::TEXT));

        for i in 1..21 {
            db.execute_mut_query(&SelectQuery::tuple(&[i.to_string(), "example".to_string()]).insert_into("document")).expect("Insert");
        }

        let result = db.execute_query(&SelectQuery::scan("document").count()).unwrap();
        let count = result.results().get(0).map(|t| t.cell_at(0)).flatten().map(|c| c.as_number()).flatten().unwrap();
        assert_eq!(count, 20);
    }
}
