use crate::select::SelectQuery;

#[derive(PartialEq, Clone)]
pub enum Token {
    Symbol(String),
    SymbolWithType(String, String),
    Pipe,
}

struct ParserState {
    tokens: Vec<Token>,
    pos: usize
}

impl ParserState {
    fn new(tokens: Vec<Token>) -> Self {
        Self{tokens, pos: 0}
    }

    fn has_next(&self) -> bool {
        self.pos < self.tokens.len()
    }

    fn next(&mut self) -> &Token {
        let next = &self.tokens[self.pos];
        self.pos += 1;
        next
    }

    fn peek(&self) -> &Token {
        &self.tokens[self.pos]
    }

    fn revind(&mut self) {
        assert!(self.pos > 0);
        self.pos -= 1
    }
}

pub fn parse_query(source: &str) -> SelectQuery {
    let mut parser = ParserState::new(tokenize(source));
    let mut query: SelectQuery = SelectQuery::scan("");

    while parser.has_next() {
        match parser.next() {
            Token::Symbol(name) => if name == "scan" {
                 query = SelectQuery::scan(expect_args(&mut parser, 1)[0].as_str())
            } else if name == "select_all" {
                query = query.select_all()
            },
            _ => todo!()
        }
    }

    query
}

fn expect_args(parser: &mut ParserState, expect: usize) -> Vec<String> {
    let mut args: Vec<String> = Vec::with_capacity(expect);

    assert!(parser.peek() != &Token::Pipe);
    match parser.next() {
        Token::Symbol(name) => args.push(name.clone()),
        _ => todo!()
    }
    assert!(parser.next().clone() == Token::Pipe);
    

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
    }

    fn assert_parse(query: &str) {
        let parsed = parse_query(query);
        assert_eq!(parsed.to_string(), query);
    }
}
