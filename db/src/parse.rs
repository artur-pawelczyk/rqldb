use crate::dsl::{Command, Query, Operator, AttrKind};
use crate::schema::Type;
use crate::tokenize::{Token, Tokenizer};

use std::fmt;
use std::str::FromStr;

#[derive(Debug)]
pub struct ParseError {
    msg: &'static str,
    pos: usize,
}

impl ParseError {
    pub fn msg(msg: &'static str) -> Self {
        Self{ msg, pos: 0 }
    }
}

impl fmt::Display for ParseError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "error at {}: {}", self.pos, self.msg)
    }
}

pub fn parse_query(query_str: &str) -> Result<Query<'_>, ParseError> {
    let mut tokenizer = Tokenizer::new(query_str);
    let mut query = read_source(&mut tokenizer)?;

    while let Some(token) = tokenizer.next() {
        match token {
            Token::Symbol(name, _) => match name {
                "select_all" => query = query.select_all(),
                "insert_into" => query = query.insert_into(read_symbol(tokenizer.next())?),
                "filter" => {
                    let left = read_symbol(tokenizer.next())?;
                    let op = read_operator(tokenizer.next())?;
                    let right = read_symbol(tokenizer.next())?;
                    if let Some(token) = tokenizer.next() {
                        check_if_end(token)?;
                    }
                    query = query.filter(left, op, right);
                },
                "join" => {
                    let table = read_symbol(tokenizer.next())?;
                    let left = read_symbol(tokenizer.next())?;
                    let right = read_symbol(tokenizer.next())?;
                    if let Some(token) = tokenizer.next() {
                        check_if_end(token)?
                    }
                    query = query.join(table, left, right);
                },
                "apply" => {
                    let function = read_symbol(tokenizer.next())?;
                    let args = read_list(&mut tokenizer)?;
                    query = query.apply(function, &args);
                }
                "count" => {
                    if let Some(token) = tokenizer.next() {
                        check_if_end(token)?;
                    }
                    query = query.count();
                },
                "delete" => query = query.delete(),
                _ => return Err(ParseError{ msg: "Function not recognized", pos: token.pos() })
            },
            _ => return Err(ParseError{ msg: "Expected a symbol", pos: token.pos() })
        }
    }

    Ok(query)
}

fn check_if_end(token: Token) -> Result<(), ParseError> {
    if !matches!(token, Token::Pipe(_)) {
        Err(ParseError::msg("Expected end of statement"))
    } else {
        Ok(())
    }
}

fn read_source<'a>(tokenizer: &mut Tokenizer<'a>) -> Result<Query<'a>, ParseError> {
    let function = match tokenizer.next() {
        Some(x) => x,
        None => return Err(ParseError::msg("Expected source function"))
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

fn read_operator(t: Option<Token>) -> Result<Operator, ParseError> {
    if let Some(token) = t {
        match token {
            Token::Symbol(s, _) => match s {
                "=" => Ok(Operator::EQ),
                ">" => Ok(Operator::GT),
                ">=" => Ok(Operator::GE),
                "<" => Ok(Operator::LT),
                "<=" => Ok(Operator::LE),
                _ => Err(ParseError{ msg: "Unknown operator", pos: token.pos() }),
            }
            _ => Err(ParseError{ msg: "Unexpected token", pos: token.pos() }),
        }
    } else {
        Err(ParseError::msg("Unknown operator"))
    }
}

fn read_symbol(t: Option<Token<'_>>) -> Result<&str, ParseError> {
    if let Some(token) = t {
        match token {
            Token::Symbol(name, _) => Ok(name),
            _ => Err(ParseError::msg("Expected a symbol"))
        }
    } else {
        Err(ParseError::msg("Expected a symbol"))
    }
}

fn read_table_scan<'a>(tokenizer: &mut Tokenizer<'a>) -> Result<Query<'a>, ParseError> {
    // let token = tokenizer.next();
    // if let Some(Token::Symbol(table, _)) = token {
    //     expect_pipe_or_end(tokenizer)?;
    //     Ok(Query::scan(table))
    // } else { // TODO: Make sure "scan" has only one argument
    //     Err(ParseError{msg: "'scan' expects a table name", pos: token.pos() })
    // }

    match tokenizer.next() {
        Some(token) => {
            if let Token::Symbol(table, _) = token {
                expect_pipe_or_end(tokenizer)?;
                Ok(Query::scan(table))
            } else { 
                Err(ParseError{ msg: "Expected a symbol", pos: token.pos() })
            }
        },
        None => Err(ParseError::msg("Expected a symbol"))
    }
}

fn read_tuple<'a>(tokenizer: &mut Tokenizer<'a>) -> Result<Vec<(AttrKind, &'a str)>, ParseError> {
    let mut values: Vec<(AttrKind, &str)> = Vec::new();

    for token in tokenizer {
        match token {
            Token::Symbol(value, _) => values.push((AttrKind::Infer, value)),
            Token::Pipe(_) => break,
            Token::SymbolWithType(value, kind, pos) => values.push((AttrKind::from_str(kind).map_err(|_| ParseError{ msg: "Unknown type", pos })?, value)),
            Token::SymbolWithKeyType(_, _, pos) => return Err(ParseError{ msg: "Expected a symbol, got typed field", pos }),
        }
    }

    Ok(values)
}

fn read_list<'a>(tokenizer: &mut Tokenizer<'a>) -> Result<Vec<&'a str>, ParseError> {
    let mut values: Vec<&str> = Vec::new();

    for token in tokenizer {
        match token {
            Token::Symbol(name, _) => values.push(name),
            Token::Pipe(_) => break,
            Token::SymbolWithType(_, _, pos) => return Err(ParseError{ msg: "Expected a symbol, got typed field", pos }),
            Token::SymbolWithKeyType(_, _, pos) => return Err(ParseError{ msg: "Expected a symbol, got typed field", pos }),
        }
    }

    Ok(values)
}

fn read_index_scan<'a>(tokenizer: &mut Tokenizer<'a>) -> Result<Query<'a>, ParseError> {
    let index = read_symbol(tokenizer.next())?;
    let op = read_symbol(tokenizer.next())?;
    let val = read_symbol(tokenizer.next())?;
    expect_pipe_or_end(tokenizer)?;

    Ok(Query::scan_index(index, op.parse()?, val))
}

fn expect_pipe_or_end(tokenizer: &mut Tokenizer) -> Result<(), ParseError> {
    let token = tokenizer.next();
    if !matches!(token, Some(Token::Pipe(_))) && token.is_some() {
        Err(ParseError{
            msg: "No more arguments were expected",
            pos: token.map(|t| t.pos()).unwrap_or(0),
        })
    } else {
        Ok(())
    }
}

pub fn parse_command(source: &str) -> Result<Command, ParseError> {
    let mut tokenizer = Tokenizer::new(source);
    let mut command = Command::create_table("");

    while let Some(token) = tokenizer.next() {
        match token {
            Token::Symbol(name, _) => if name == "create_table" {
                let name = match tokenizer.next() {
                    Some(Token::Symbol(name, _)) => name,
                    _ => return Err(ParseError::msg("Expected table name"))
                };
                command = Command::create_table(name);

                for arg in tokenizer.by_ref() {
                    match arg {
                        Token::SymbolWithType(name, kind, _) => { command = command.column(name, str_to_type(kind)?); },
                        Token::SymbolWithKeyType(name, kind, _) => { command = command.indexed_column(name, str_to_type(kind)?); },
                        _ => return Err(ParseError::msg("Expected a symbol with a type"))
                    }
                }
            } else { return Err(ParseError::msg("Only create_table is allowed")) },
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

        ($query:literal, $pos:literal) => {
            {
                let parsed = parse_query($query);
                dbg!(&parsed);
                assert!(matches!(parsed, Err(ParseError{ msg: _, pos: $pos })));
            }
        }
    }

    #[test]
    fn test_parse_query() {
        assert_parse!("scan example | select_all");
        assert_parse!("tuple 1 2 | select_all");
        assert_parse!("tuple 1 2 | insert_into example");
        assert_parse!("scan example | filter id = 1 | select_all");
        assert_parse!("scan example | filter id > 1 | select_all");
        assert_parse!("scan example | join other example.other_id example.id | select_all");
        assert_parse!("scan example | count");
        assert_parse!("scan example | filter example.id = 1 | delete");
        assert_parse!("scan_index example.id = 1 | select_all");
        assert_parse!("scan example | apply sum example.n");
    }

    #[test]
    fn test_parse_typed_tuple() {
        assert_parse!("tuple 1::NUMBER something::TEXT | select_all");
    }

    #[test]
    fn test_fail_parse_query() {
        assert_parse_query_fails!("");
        assert_parse_query_fails!("scan | select_all", 5);
        assert_parse_query_fails!("scann example");
        assert_parse_query_fails!("scan example1 example2", 14);
        assert_parse_query_fails!("| select_all");
        assert_parse_query_fails!("scan example | i_dont_know", 15);
        assert_parse_query_fails!("scan example | id::NUMBER", 15);
        assert_parse_query_fails!("scan example | filter id ! 1", 25);
    }

    #[test]
    fn test_parse_command() {
        assert_parse_command("create_table example id::NUMBER content::TEXT");
        assert_parse_command("create_table example id::NUMBER::KEY content::TEXT");
    }

    #[test]
    fn test_fails_parse_command() {
        assert_parse_command_fails("create_someting example");
        assert_parse_command_fails("| create_table example");
        assert_parse_command_fails("create_table int::NUMBER example contents::TEXT");
        assert_parse_command_fails("create_table example int::NUMBER | contents::TEXT");
        assert_parse_command_fails("create_table example int::NUMBER something contents::TEXT");
    }

    fn assert_parse_command(command: &str) {
        let parsed = parse_command(command);
        assert_eq!(parsed.unwrap().to_string(), command);
    }

    fn assert_parse_command_fails(command: &str) {
        let parsed = parse_command(command);
        assert!(parsed.is_err());
    }
}
