use crate::select::SelectQuery;
use crate::create::CreateRelationCommand;
use crate::schema::Type;

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

    fn next(&mut self) -> Option<&Token> {
        if self.has_next() {
            let next = &self.tokens[self.pos];
            self.pos += 1;
            Option::Some(next)
        } else {
            Option::None
        }
    }

    fn _peek(&self) -> &Token {
        &self.tokens[self.pos]
    }

    fn _rewind(&mut self) {
        assert!(self.pos > 0);
        self.pos -= 1
    }
}

pub fn parse_query(source: &str) -> SelectQuery {
    let mut parser = ParserState::new(tokenize(source));
    let mut query = SelectQuery::scan("");

    while let Some(token) = parser.next() {
        match token {
            Token::Symbol(name) => if name == "scan" {
                let rest = expect_n_args(read_until_end(&mut parser), 1);
                for arg in rest {
                    match arg {
                        Token::Symbol(name) => { query = SelectQuery::scan(name.as_str()) },
                        _ => panic!()
                    }
                }

            } else if name == "select_all" {
                query = query.select_all()
            },
            _ => todo!()
        }
    }

    query
}

pub fn parse_command(source: &str) -> CreateRelationCommand {
    let mut parser = ParserState::new(tokenize(source));
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

fn read_until_end(parser: &mut ParserState) -> Vec<Token> {
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
