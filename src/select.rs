use std::fmt;

pub struct SelectQuery {
    pub source: Source,
    pub filters: Vec<Filter>,
    pub finisher: Finisher
}

impl SelectQuery {
    pub fn scan(table: &str) -> Self {
        SelectQuery{source:  Source::TableScan(String::from(table)), filters: Vec::new(), finisher: Finisher::AllColumns}
    }

    pub fn tuple(values: &[String]) -> Self {
        SelectQuery{source: Source::Tuple(values.iter().map(|x| x.to_string()).collect()), filters: Vec::new(), finisher: Finisher::AllColumns}
    }

    pub fn filter(mut self, left: &str, op: Operator, right: &str) -> Self {
        self.filters.push(Filter::Condition(left.to_string(), op, right.to_string()));
        return self;
    }

    pub fn select_all(mut self) -> Self {
        self.finisher = Finisher::AllColumns;
        self
    }

    pub fn select(mut self, columns: &[&str]) -> Self {
        self.finisher = Finisher::Columns(columns.iter().map(|x| x.to_string()).collect());
        self
    }

    pub fn insert_into(mut self, name: &str) -> Self {
        self.finisher = Finisher::Insert(name.to_string());
        self
    }

    pub fn print(&self) -> String {
        let mut s = String::new();

        s.push_str(&self.source.print());

        for filter in &self.filters {
            s.push_str(" | ");
            s.push_str(&filter.print());
        }

        s.push_str(" | ");
        s.push_str(&self.finisher.print());

        s
    }
}

impl fmt::Display for SelectQuery {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.print())
    }
}
    
pub enum Operator {
    EQ
}

impl Operator {
    fn print(&self) -> String {
        match self {
            Operator::EQ => "=".to_string()
        }
    }
}

pub enum Source {
    TableScan(String),
    Tuple(Vec<String>)
}

impl Source {
    fn print(&self) -> String {
        match self {
            Source::TableScan(table) => "scan ".to_owned() + table,
            Source::Tuple(values) => "tuple ".to_owned() + &print_tokens(values)
        }
    }
}

pub enum Filter {
    Condition(String, Operator, String)
}

impl Filter {
    fn print(&self) -> String {
        match self {
            Filter::Condition(left, op, right) => "filter ".to_owned() + left + " " + &op.print() + " " + right
        }
    }
}

pub enum Finisher {
    AllColumns,
    Columns(Vec<String>),
    Insert(String)
}

impl Finisher {
    fn print(&self) -> String {
        match self {
            Finisher::AllColumns => "select_all".to_string(),
            Finisher::Columns(rows) => "select ".to_string() + &print_tokens(rows),
            Finisher::Insert(name) => "insert_into ".to_string() + name
        }
    }
}

fn print_tokens(tokens: &[String]) -> String {
    let mut s  = String::new();
    for token in tokens {
        s.push_str(token);
        s.push(' ');
    }

    if !s.is_empty() { s.pop(); }
    
    s
}

#[cfg(test)]
mod tests {
    use super::SelectQuery;
    use super::Operator::EQ;

    #[test]
    fn select_all() {
         let query = SelectQuery::scan("example").select_all();
        assert_eq!(query.to_string(), "scan example | select_all")
    }

    #[test]
    fn where_clause() {
        let query = SelectQuery::scan("example").filter("id", EQ, "1").select(&["id", "a_column"]);
        assert_eq!(query.to_string(), "scan example | filter id = 1 | select id a_column")
    }

    #[test]
    fn source_is_tuple() {
        let query = SelectQuery::tuple(&["1".to_string(), "example_value".to_string()]).insert_into("example");
        assert_eq!(query.to_string(), "tuple 1 example_value | insert_into example");
    }
}
