use crate::select::SelectQuery;
use crate::create::CreateRelationCommand;
use crate::schema::Type;

use std::rc::Rc;

#[derive(PartialEq, Clone)]
pub enum Token {
    Symbol(String),
    SymbolWithType(String, String),
    Pipe,
}

impl Token {
    fn to_string(&self) -> String {
        match self {
            Token::Symbol(x) => String::from(x),
            Token::SymbolWithType(x, y) => x.clone() + "::" + y,
            Token::Pipe => String::from("|"),
        }
    }
}

struct Cursor {
    tokens: Rc<Vec<Token>>,
    pos: usize
}

impl Cursor {
    fn new(tokens: Vec<Token>) -> Self {
        Self{tokens: Rc::new(tokens), pos: 0}
    }

    fn clone(&self) -> Self {
        Self{tokens: Rc::clone(&self.tokens), pos: self.pos}
    }

    fn has_next(&self) -> bool {
        self.pos < self.tokens.len()
    }

    fn next(&mut self) -> Option<&Token> {
        if self.has_next() {
            let next = &self.tokens[self.pos];
            self.pos += 1;
            Option::Some(next)
        } else {
            Option::None
        }
    }

    fn peek(&self) -> Option<&Token> {
        self.tokens.get(self.pos)
    }

    fn _rewind(&mut self) {
        assert!(self.pos > 0);
        self.pos -= 1
    }
}

pub fn parse_query(query_str: &str) -> SelectQuery {
    let mut cursor = Cursor::new(tokenize(query_str));
    let mut query = read_source(&mut cursor);

    while let Some(token) = cursor.next() {
        match token {
            Token::Symbol(name) => match name.as_str() {
                "select_all" => query = query.select_all(),
                "insert_into" => query = query.insert_into(cursor.next().unwrap().to_string().as_str()),
                _ => panic!()
            },
            _ => panic!()
        }
    }

    query
}

fn read_source(cursor: &mut Cursor) -> SelectQuery {
    let function = cursor.peek().expect("Expected source function");

    match function {
        Token::Symbol(name) => match name.as_str() {
            "scan" => read_table_scan(cursor),
            "tuple" => read_tuple(cursor),
            _ => panic!()
        },
        _ => panic!()
    }
}

fn read_table_scan(cursor: &mut Cursor) -> SelectQuery {
    cursor.next().expect("Expected  'scan' function");
    let args = read_until_end(cursor);
    SelectQuery::scan(args.get(0).expect("Expected one argument to 'scan'").to_string().as_str())
}

fn read_tuple(cursor: &mut Cursor) -> SelectQuery {
    cursor.next().expect("Expected 'tuple' function");

    let mut values = Vec::new();
    for token in read_until_end(cursor) {
        values.push(token.to_string());
    }

    SelectQuery::tuple(&values)
}

pub fn parse_command(source: &str) -> CreateRelationCommand {
    let mut parser = Cursor::new(tokenize(source));
    let mut command = CreateRelationCommand::with_name("");

    while let Some(token) = parser.next() {
        match token {
            Token::Symbol(name) => if name == "create_table" {
                let rest = read_until_end(&mut parser);
                for arg in rest {
                    match arg {
                        Token::Symbol(name) => { command = CreateRelationCommand::with_name(name.as_str()); },
                        Token::SymbolWithType(name, kind) => { command = command.column(name.as_str(), str_to_type(kind.as_str())); },
                        _ => panic!()
                    }
                }
            } else { panic!("Only create_table is allowed") },
            _ => panic!()
        }
    }

    command
}

fn str_to_type(name: &str) -> Type {
    match name {
        "NUMBER" => Type::NUMBER,
        "TEXT" => Type::TEXT,
        _ => todo!(),
    }
}

fn expect_n_args<T>(args: Vec<T>, expect: usize) -> Vec<T> {
    let len = args.len();
    if len != expect {
        panic!("Expected {} args; given {}", expect, len);
    }

    args
}

fn read_until_end(parser: &mut Cursor) -> Vec<Token> {
    let mut args: Vec<Token> = Vec::new();

    while let Some(token) = parser.next() {
        match token {
            Token::Pipe => { break },
            _ => args.push(token.clone())
        }
    }

    args
}

fn tokenize(source: &str) -> Vec<Token> {
    source.split_ascii_whitespace()
        .map(|x| match_token(x))
        .collect()
}

fn match_token(source: &str) -> Token {
    if source == "|" {
        Token::Pipe
    } else if source.contains("::") {
        let mut parts = source.split("::");
        let name = parts.next().unwrap();
        let kind = parts.next().unwrap();
        Token::SymbolWithType(String::from(name), String::from(kind))
    } else {
        Token::Symbol(String::from(source))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    

    #[test]
    fn test_parse_query() {
        assert_parse("scan example | select_all");
        assert_parse("tuple 1 2 | select_all");
        assert_parse("tuple 1 2 | insert_into example");
    }

    fn assert_parse(query: &str) {
        let parsed = parse_query(query);
        assert_eq!(parsed.to_string(), query);
    }

    #[test]
    fn test_parse_command() {
        assert_parse_command("create_table example id::NUMBER content::TEXT");
    }

    fn assert_parse_command(command: &str) {
        let parsed = parse_command(command);
        assert_eq!(parsed.to_string(), command);
    }
}
