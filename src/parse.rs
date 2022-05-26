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

    fn next(&mut self) -> &Token {
        let next = &self.tokens[self.pos];
        self.pos += 1;
        next
    }

    fn next2(&mut self) -> Option<&Token> {
        if self.has_next() {
            Option::Some(self.next())
        } else {
            Option::None
        }
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
    let mut query = SelectQuery::scan("");

    while parser.has_next() {
        match parser.next() {
            Token::Symbol(name) => if name == "scan" {
                 query = SelectQuery::scan(read_args(&mut parser)[0].name())
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

    while let Some(token) = parser.next2() {
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

struct Arg {
    name: String,
    kind: Option<String>
}

impl Arg {
    fn simple(name: &str) -> Self {
        Self{name: name.to_string(), kind: Option::None}
    }

    fn with_type(name: &str, kind: &str) -> Self {
        Self{name: name.to_string(), kind: Option::Some(kind.to_string())}
    }

    fn name(&self) -> &str {
        self.name.as_str()
    }

    fn expect_type(&self) -> &str {
        match &self.kind {
            Some(kind) => kind.as_str(),
            _ => panic!()
        }
    }
}

fn read_args(parser: &mut ParserState) -> Vec<Arg> {
    let mut args: Vec<Arg> = Vec::new();

    while let Some(arg) = parser.next2() {
        if arg == &Token::Pipe { break; }

        match arg {
            Token::Symbol(name) => args.push(Arg::simple(name)),
            Token::SymbolWithType(name, kind) => args.push(Arg::with_type(name, kind)),
            _ => panic!()
        }
    }

    return args;
}

fn read_until_end(parser: &mut ParserState) -> Vec<Token> {
    let mut args: Vec<Token> = Vec::new();

    while let Some(token) = parser.next2() {
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
        assert_parse_command("create_table example id::NUMBER");
    }

    fn assert_parse_command(command: &str) {
        let parsed = parse_command(command);
        assert_eq!(parsed.to_string(), command);
    }
}
