use crate::dsl::{Definition, Delete, Insert, IntoTuple, Operator, Query, TupleAttr, TupleBuilder};
use crate::schema::Type;
use crate::tokenize::{Token, Tokenizer, TokenizerError};

use std::fmt;
use std::str::FromStr;

#[derive(Debug)]
pub enum ParseError {
    Msg(&'static str, usize),
    UnexpectedToken(Box<str>, Box<str>, usize),
}

impl ParseError {
    pub(crate) fn msg(msg: &'static str) -> Self {
        Self::Msg(msg, 0)
    }

    pub(crate) fn expected(self, expected: &str) -> Self {
        match self {
            Self::UnexpectedToken(actual, _, pos) => Self::UnexpectedToken(actual, Box::from(expected), pos),
            x => x,
        }
    }
}

impl std::error::Error for ParseError {
}

impl fmt::Display for ParseError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Msg(msg, pos) => write!(f, "{msg} at {pos}"),
            Self::UnexpectedToken(actual, expected, pos) if expected.is_empty() => write!(f, "Unexpected token {actual} at {pos}"),
            Self::UnexpectedToken(actual, expected, pos) => write!(f, "Expected {actual}, got {expected} at {pos}"),
        }
    }
}

impl From<TokenizerError> for ParseError {
    fn from(_: TokenizerError) -> Self {
        Self::msg("tokenizer error")
    }
}

impl From<Token<'_>> for ParseError {
    fn from(token: Token<'_>) -> Self {
        Self::UnexpectedToken(Box::from(token.to_string()), Box::from(""), token.pos())
    }
}

pub fn parse_query(query_str: &str) -> Result<Query<'_>, ParseError> {
    let mut tokenizer = Tokenizer::new(query_str);
    let mut query = read_source(&mut tokenizer)?;

    loop {
        let token = tokenizer.next()?;
        match token {
            Token::Symbol(name, _) => match name {
                "select_all" => query = query.select_all(),
                "insert_into" => unimplemented!(),
                "filter" => {
                    let left = read_symbol(tokenizer.next()?)?;
                    let op = read_operator(tokenizer.next()?)?;
                    let right = read_symbol(tokenizer.next()?)?;
                    check_if_end(tokenizer.next()?)?;
                    query = query.filter(left, op, right);
                },
                "join" => {
                    let left = read_symbol(tokenizer.next()?)?;
                    let right = read_symbol(tokenizer.next()?)?;
                    check_if_end(tokenizer.next()?)?;
                    query = query.join(left, right);
                },
                "apply" => {
                    let function = read_symbol(tokenizer.next()?)?;
                    let args = read_list(&mut tokenizer)?;
                    query = query.apply(function, &args);
                }
                "count" => {
                    check_if_end(tokenizer.next()?)?;
                    query = query.count();
                },
                "delete" => unimplemented!(),
                _ => return Err(ParseError::Msg("Function not recognized", token.pos()))
            },
            Token::End(_) => break,
            _ => return Err(ParseError::from(token).expected("symbol"))
        }
    }

    Ok(query)
}

pub fn parse_insert<'a>(query_str: &'a str) -> Result<Insert<'a>, ParseError> {
    let mut tokenizer = Tokenizer::new(query_str);
    let target = tokenizer.next().map_err(|token| ParseError::from(token).expected("relation name"))?;
    match target {
        Token::Symbol(target, _) => {
            Ok(Insert::insert_into(target).tuple(read_tuple(&mut tokenizer)?))
        },
        t => Err(ParseError::from(t))
    }
}

pub fn parse_delete(query_str: &str) -> Result<Delete<'_>, ParseError> {
    parse_query(query_str).map(|q| Delete(q))
}

fn check_if_end(token: Token) -> Result<(), ParseError> {
    match token {
        Token::Pipe(_) => Ok(()),
        Token::End(_) => Ok(()),
        _ => Err(ParseError::msg("Expected end of statement")),
    }
}

fn read_source<'a>(tokenizer: &mut Tokenizer<'a>) -> Result<Query<'a>, ParseError> {
    let function = match tokenizer.next()? {
        Token::End(_) => return Err(ParseError::msg("Expected source function, found end of stream")),
        x => x,
    };

    match function {
        Token::Symbol(name, _) => match name {
            "scan" => read_table_scan(tokenizer),
            "tuple" => read_tuple(tokenizer).map(|values| Query::tuple(&values)),
            "scan_index" => read_index_scan(tokenizer),
            _ => Result::Err(ParseError::msg("Unnokwn source function")),
        },
        _ => Err(ParseError::msg("Expected a symbol"))
    }
}

fn read_operator(t: Token) -> Result<Operator, ParseError> {
    match t {
        Token::Op(s, _) => match s {
            "=" => Ok(Operator::EQ),
            ">" => Ok(Operator::GT),
            ">=" => Ok(Operator::GE),
            "<" => Ok(Operator::LT),
            "<=" => Ok(Operator::LE),
            _ => Err(ParseError::Msg("Unknown operator", t.pos())),
        }
        t => Err(ParseError::from(t).expected("operator"))
    }
}

fn read_symbol(t: Token<'_>) -> Result<&str, ParseError> {
    match t {
        Token::Symbol(name, _) => Ok(name),
        _ => Err(ParseError::msg("Expected a symbol"))
    }
}

fn read_table_scan<'a>(tokenizer: &mut Tokenizer<'a>) -> Result<Query<'a>, ParseError> {
    match tokenizer.next()? {
        Token::Symbol(table, _) => {
            expect_pipe_or_end(tokenizer)?;
            Ok(Query::scan(table))
        },
        token => Err(ParseError::from(token).expected("symbol"))
    }
}

fn read_tuple<'a>(tokenizer: &mut Tokenizer<'a>) -> Result<Vec<TupleAttr<'a>>, ParseError> {
    let mut tuple_builder = TupleBuilder::new();

    loop {
        let token = tokenizer.next()?;
        match token {
            Token::Symbol(value_or_name, _) => {
                match tokenizer.next()? {
                    Token::Op("=", _) => {
                        if let Token::Symbol(value, _) = tokenizer.next()? {
                            tuple_builder = tuple_builder.inferred(value_or_name, value);
                        } else {
                            return Err(ParseError::msg("Expected a symbol"));
                        }
                    },
                    token => return Err(ParseError::from(token).expected("equals sign"))
                }
            },
            Token::Pipe(_) => break,
            Token::End(_) => break,
            Token::SymbolWithType(value_or_name, kind, pos) => {
                let kind = Type::from_str(kind).map_err(|_| ParseError::Msg("Unknown type", pos))?;
                match tokenizer.next()? {
                    Token::Op("=", _) => {
                        if let Token::Symbol(value, _) = tokenizer.next()? {
                            tuple_builder = tuple_builder.typed(kind, value_or_name, value);
                        } else {
                            return Err(ParseError::msg("Expected a symbol"));
                        }
                    },
                    token => return Err(ParseError::from(token).expected("equals sign"))
                }
            },
            Token::SymbolWithKeyType(_, _, _) => return Err(ParseError::from(token).expected("symbol")),
            t => return Err(ParseError::from(t))
        }
    }

    Ok(tuple_builder.into_tuple())
}

fn read_list<'a>(tokenizer: &mut Tokenizer<'a>) -> Result<Vec<&'a str>, ParseError> {
    let mut values: Vec<&str> = Vec::new();

    loop {
        let token = tokenizer.next()?;
        match token {
            Token::Symbol(name, _) => values.push(name),
            Token::Pipe(_) => break,
            Token::End(_) => break,
            Token::SymbolWithType(_, _, _) => return Err(ParseError::from(token).expected("typed field")),
            Token::SymbolWithKeyType(_, _, _) => return Err(ParseError::from(token).expected("typed field")),
            Token::Op(_, _) => todo!(),
        }
    }

    Ok(values)
}

fn read_index_scan<'a>(tokenizer: &mut Tokenizer<'a>) -> Result<Query<'a>, ParseError> {
    let index = read_symbol(tokenizer.next()?)?;
    let op = read_operator(tokenizer.next()?)?;
    let val = read_symbol(tokenizer.next()?)?;
    expect_pipe_or_end(tokenizer)?;

    Ok(Query::scan_index(index, op, val))
}

fn expect_pipe_or_end(tokenizer: &mut Tokenizer) -> Result<(), ParseError> {
    let token = tokenizer.next()?;
    match token {
        Token::Pipe(_) => Ok(()),
        Token::End(_) => Ok(()),
        t => Err(ParseError::Msg("No more arguments were expected", t.pos())),
    }
}

pub fn parse_definition(source: &str) -> Result<Definition, ParseError> {
    let mut tokenizer = Tokenizer::new(source);
    let mut command = Definition::relation("");

    loop {
        let token = tokenizer.next()?;
        match token {
            Token::Symbol(name, _) => {
                if name == "relation" {
                    let name = match tokenizer.next()? {
                        Token::Symbol(name, _) => name,
                        _ => return Err(ParseError::msg("Expected table name"))
                    };
                    command = Definition::relation(name);

                    loop {
                        let arg = tokenizer.next()?;
                        match arg {
                            Token::SymbolWithType(name, kind, _) => { command = command.attribute(name, str_to_type(kind)?); },
                            Token::SymbolWithKeyType(name, kind, _) => { command = command.indexed_attribute(name, str_to_type(kind)?); },
                            Token::End(_) => break,
                            _ => return Err(ParseError::msg("Expected a symbol with a type")),
                        }
                    }
                } else {
                    return Err(ParseError::msg("Only relation can be defined"))
                }
            },
            Token::End(_) => break,
            _ => return Err(ParseError::msg("Expected a symbol"))
        }
    }

    Ok(command)
}

fn str_to_type(name: &str) -> Result<Type, ParseError> {
    Type::from_str(name).map_err(|_| ParseError::msg("Unrecognized type"))
}

#[cfg(test)]
mod tests {
    use super::*;

    macro_rules! assert_parse {
        ($query:literal) => {
            {
                let parsed = dbg!(parse_query($query)).unwrap();
                assert_eq!(parsed.to_string(), $query);
            }
        };
    }

    macro_rules! assert_parse_query_fails {
        ($query:literal) => {
            {
                let parsed = parse_query($query);
                dbg!(&parsed);
                assert!(parsed.is_err());
            }
        };
    }

    macro_rules! assert_parse_insert {
        ($query:literal) => {
            {
                let parsed = dbg!(parse_insert($query)).unwrap();
                assert_eq!(parsed.to_string(), $query);
            }
        };
    }

    #[test]
    fn test_parse_query() {
        assert_parse!("scan example | select_all");
        assert_parse!("tuple first = 1 second = 2 | select_all");
        assert_parse!("tuple id::NUMBER = 1 name::TEXT = something | select_all");
        assert_parse!("scan example | filter id = 1 | select_all");
        assert_parse!("scan example | filter id > 1 | select_all");
        assert_parse!("scan example | join example.other_id other.id | select_all");
        assert_parse!("scan example | count");
        assert_parse!("scan_index example.id = 1 | select_all");
        assert_parse!("scan example | apply sum example.n");
    }

    #[test]
    fn test_fail_parse_query() {
        assert_parse_query_fails!("");
        assert_parse_query_fails!("scan | select_all");
        assert_parse_query_fails!("scann example");
        assert_parse_query_fails!("scan example1 example2");
        assert_parse_query_fails!("| select_all");
        assert_parse_query_fails!("scan example | i_dont_know");
        assert_parse_query_fails!("scan example | id::NUMBER");
        assert_parse_query_fails!("scan example | filter id ! 1");
        assert_parse_query_fails!("tuple 1 2 | select_all");
    }

    #[test]
    fn test_parse_insert() {
        assert_parse_insert!("example id = 1 name = something");
    }

    #[test]
    fn test_parse_definition() {
        assert_parse_definition("relation example id::NUMBER content::TEXT");
        assert_parse_definition("relation example id::NUMBER::KEY content::TEXT");
    }

    #[test]
    fn test_fails_parse_definition() {
        assert_parse_definition_fails("something example");
        assert_parse_definition_fails("| relation example");
        assert_parse_definition_fails("relation int::NUMBER example contents::TEXT");
        assert_parse_definition_fails("relation example int::NUMBER | contents::TEXT");
        assert_parse_definition_fails("relation example int::NUMBER something contents::TEXT");
    }

    fn assert_parse_definition(command: &str) {
        let parsed = parse_definition(dbg!(command));
        assert_eq!(parsed.unwrap().to_string(), command);
    }

    fn assert_parse_definition_fails(command: &str) {
        let parsed = parse_definition(command);
        assert!(parsed.is_err());
    }
}
