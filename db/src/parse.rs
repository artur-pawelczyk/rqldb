use crate::select::{SelectQuery, Operator};
use crate::create::CreateRelationCommand;
use crate::schema::Type;
use crate::tokenize::{Token, Tokenizer};

use std::fmt;

#[derive(Debug)]
pub struct ParseError(&'static str);

impl fmt::Display for ParseError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.0)
    }
}

pub fn parse_query(query_str: &str) -> Result<SelectQuery, ParseError> {
    let mut tokenizer = Tokenizer::from_str(query_str);
    let mut query = read_source(&mut tokenizer)?;

    while let Some(token) = tokenizer.next() {
        match token {
            Token::Symbol(name) => match name.as_str() {
                "select_all" => query = query.select_all(),
                "insert_into" => query = query.insert_into(tokenizer.next().unwrap().to_string().as_str()),
                "filter" => {
                    let left = read_symbol(tokenizer.next())?;
                    let op = read_operator(tokenizer.next())?;
                    let right = read_symbol(tokenizer.next())?;
                    if let Some(token) = tokenizer.next() {
                        if token != &Token::Pipe { return Err(ParseError("Expected end of statement")); }
                    }
                    query = query.filter(&left, op, &right);
                },
                "join" => {
                    let table = read_symbol(tokenizer.next())?;
                    let left = read_symbol(tokenizer.next())?;
                    let right = read_symbol(tokenizer.next())?;
                    if let Some(token) = tokenizer.next() {
                        if token != &Token::Pipe { return Err(ParseError("Expected end of statement")); }
                    }
                    query = query.join(&table, &left, &right);
                },
                "count" => {
                    if let Some(token) = tokenizer.next() {
                        if token != &Token::Pipe { return Err(ParseError("Expected end of statement")); }
                    }
                    query = query.count();
                }
                _ => return Err(ParseError("Function not recognized"))
            },
            _ => return Err(ParseError("Expected a symbol"))
        }
    }

    Ok(query)
}

fn read_source(tokenizer: &mut Tokenizer) -> Result<SelectQuery, ParseError> {
    let function = match tokenizer.next() {
        Some(x) => x,
        None => return Err(ParseError("Expected source function"))
    };

    match function {
        Token::Symbol(name) => match name.as_str() {
            "scan" => read_table_scan(tokenizer),
            "tuple" => read_tuple(tokenizer),
            _ => Result::Err(ParseError("Unnokwn source function")),
        },
        _ => Err(ParseError("Expected a symbol"))
    }
}

fn read_operator(t: Option<&Token>) -> Result<Operator, ParseError> {
    if let Some(token) = t {
        match token {
            Token::Symbol(s) => match s.as_str() {
                "=" => Ok(Operator::EQ),
                ">" => Ok(Operator::GT),
                ">=" => Ok(Operator::GE),
                "<" => Ok(Operator::LT),
                "<=" => Ok(Operator::LE),
                _ => Err(ParseError("Unknown operator")),
            }
            _ => Err(ParseError("Unexpected token")),
        }
    } else {
        Err(ParseError("Unknown operator"))
    }
}

fn read_symbol(t: Option<&Token>) -> Result<String, ParseError> {
    if let Some(token) = t {
        match token {
            Token::Symbol(name) => Ok(name.to_string()),
            _ => Err(ParseError("Expected a symbol"))
        }
    } else {
        Err(ParseError("Expected a symbol"))
    }
}

fn read_table_scan(tokenizer: &mut Tokenizer) -> Result<SelectQuery, ParseError> {
    let args = read_until_end(tokenizer);
    if args.len() == 1 {
        Ok(SelectQuery::scan(&args[0].to_string()))
    } else {
        Err(ParseError("'scan' expects exactly one argument"))
    }
}

fn read_tuple(tokenizer: &mut Tokenizer) -> Result<SelectQuery, ParseError> {
    let mut values: Vec<String> = Vec::new();
    for token in read_until_end(tokenizer) {
        match token {
            Token::Symbol(name) => values.push(name.to_string()),
            _ => return Err(ParseError("Expected a symbol"))
        }
    }

    Ok(SelectQuery::tuple(&values))
}

pub fn parse_command(source: &str) -> Result<CreateRelationCommand, ParseError> {
    let mut tokenizer = Tokenizer::from_str(source);
    let mut command = CreateRelationCommand::with_name("");

    while let Some(token) = tokenizer.next() {
        match token {
            Token::Symbol(name) => if name == "create_table" {
                let name = match tokenizer.next() {
                    Some(Token::Symbol(name)) => name,
                    _ => return Err(ParseError("Expected table name"))
                };
                command = CreateRelationCommand::with_name(name.as_str());

                for arg in read_until_end(&mut tokenizer) {
                    match arg {
                        Token::SymbolWithType(name, kind) => { command = command.column(name.as_str(), str_to_type(kind.as_str())); },
                        _ => return Err(ParseError("Expected a symbol with a type"))
                    }
                }
            } else { return Err(ParseError("Only create_table is allowed")) },
            _ => return Err(ParseError("Expected a symbol"))
        }
    }

    Ok(command)
}

fn str_to_type(name: &str) -> Type {
    match name {
        "NUMBER" => Type::NUMBER,
        "TEXT" => Type::TEXT,
        _ => panic!("Unknown type {}", name)
    }
}

fn read_until_end(tokenizer: &mut Tokenizer) -> Vec<Token> {
    let mut args: Vec<Token> = Vec::new();

    while let Some(token) = tokenizer.next() {
        match token {
            Token::Pipe => { break },
            _ => args.push(token.clone())
        }
    }

    args
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_query() {
        assert_parse("scan example | select_all");
        assert_parse("tuple 1 2 | select_all");
        assert_parse("tuple 1 2 | insert_into example");
        assert_parse("scan example | filter id = 1 | select_all");
        assert_parse("scan example | filter id > 1 | select_all");
        assert_parse("scan example | join other example.other_id example.id | select_all");
        assert_parse("scan example | count");
    }

    #[test]
    fn test_fail_parse_query() {
        assert_parse_query_fails("");
        assert_parse_query_fails("scann example");
        assert_parse_query_fails("scan example1 example2");
        assert_parse_query_fails("| select_all");
        assert_parse_query_fails("scan example | i_dont_know");
        assert_parse_query_fails("scan example | id::NUMBER");
    }

    fn assert_parse(query: &str) {
        let parsed = parse_query(query);
        assert_eq!(parsed.unwrap().to_string(), query);
    }

    fn assert_parse_query_fails(query: &str) {
        let parsed = parse_query(query);
        assert!(parsed.is_err());
    }

    #[test]
    fn test_parse_command() {
        assert_parse_command("create_table example id::NUMBER content::TEXT");
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
