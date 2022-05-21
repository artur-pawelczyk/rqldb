use std::fmt;

pub struct SelectQuery {
    source: Source,
    filters: Vec<Filter>,
    finisher: Finisher
}

impl SelectQuery {
    pub fn scan(table: &str) -> Self {
        SelectQuery{source:  Source::TableScan(String::from(table)), filters: Vec::new(), finisher: Finisher::AllRows}
    }

    pub fn filter(mut self, left: &str, op: Operator, right: &str) -> Self {
        self.filters.push(Filter::Condition(left.to_string(), op, right.to_string()));
        return self;
    }

    pub fn select_all(mut self) -> Self {
        self.finisher = Finisher::AllRows;
        self
    }

    pub fn select(mut self, columns: &[&str]) -> Self {
        self.finisher = Finisher::Rows(columns.iter().map(|x| x.to_string()).collect());
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

enum Source {
    TableScan(String)
}

impl Source {
    fn print(&self) -> String {
        match self {
            Source::TableScan(table) => "scan ".to_owned() + table
        }
    }
}

enum Filter {
    Condition(String, Operator, String)
}

impl Filter {
    fn print(&self) -> String {
        match self {
            Filter::Condition(left, op, right) => "filter ".to_owned() + &join(left, &op.print(), right)
        }
    }
}

fn join(a: &str, b: &str, c: &str) -> String {
    [a, " ", b, " ", c].iter().copied().collect::<String>()
}

enum Finisher {
    AllRows, Rows(Vec<String>)
}

impl Finisher {
    fn print(&self) -> String {
        match self {
            Finisher::AllRows => "select_all".to_owned(),
            Finisher::Rows(rows) => "select ".to_owned() + &join2(rows)
        }
    }
}

fn join2(tokens: &[String]) -> String {
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
}
