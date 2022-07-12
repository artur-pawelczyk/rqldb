#[derive(Clone, Debug, PartialEq)]
pub enum Token {
    Symbol(String),
    SymbolWithType(String, String),
    SymbolWithKeyType(String, String),
    Pipe,
}

pub struct Tokenizer {
    tokens: Vec<Token>,
    pos: usize,
}

impl Tokenizer {
    pub fn from_str(source: &str) -> Self {
        Self{ tokens: tokenize(source).unwrap(), pos: 0 }
    }

    pub fn next(&mut self) -> Option<&Token> {
        let next = self.tokens.get(self.pos);
        self.pos += 1;
        next
    }
}

impl Token {
    pub fn to_string(&self) -> String {
        match self {
            Token::Symbol(x) => x.to_string(),
            Token::SymbolWithType(x, y) => x.to_string() + "::" + y,
            Token::SymbolWithKeyType(x, y) => x.to_string() + "::" + y + "::KEY",
            Token::Pipe => String::from("|"),
        }
    }
}

struct Scanner {
    chars: Vec<char>,
    pos: usize,
}

impl Scanner {
    fn from_str(source: &str) -> Self {
        Self{ chars: source.chars().collect(), pos: 0 }
    }

    fn next(&mut self) -> Option<char> {
        let next = self.chars.get(self.pos).copied();
        self.pos += 1;
        next
    }

    fn push_back(&mut self) {
        if self.pos > 0 {
            self.pos -= 1;
        }
    }

    fn expect(&mut self, expected: char) -> bool {
        if let Some(ch) = self.next() {
            ch == expected
        } else {
            false
        }
    }

    fn expect_str(&mut self, expected: &str) -> bool {
        expected.chars().all(|c| self.expect(c))
    }
}

fn tokenize(source: &str) -> Option<Vec<Token>> {
    let mut tokens = vec![];
    let mut scanner = Scanner::from_str(source);
    while let Some(ch) = scanner.next() {
        if ch == '|' {
            tokens.push(Token::Pipe);
        } else if ch == '"' {
            scanner.push_back();
            tokens.push(read_string(&mut scanner)?);
        } else if !ch.is_whitespace() {
            scanner.push_back();
            tokens.push(read_symbol(&mut scanner)?);
        }
    }

    Some(tokens)
}

fn read_symbol(scanner: &mut Scanner) -> Option<Token> {
    let mut s = String::new();
    while let Some(ch) = scanner.next() {
        if ch == ':' {
            if scanner.expect(':') {
                return match read_type(scanner) {
                    (Some(t), false) => Some(Token::SymbolWithType(s, t)),
                    (Some(t), true) => Some(Token::SymbolWithKeyType(s, t)),
                    _ => None,
                }
            } else {
                return None
            }
        } else if ch.is_whitespace() {
            break;
        } else {
            s.push(ch);
        }
    }

    assert!(!s.is_empty());
    Some(Token::Symbol(s))
}

fn read_type(scanner: &mut Scanner) -> (Option<String>, bool) {
    let mut s = String::new();

    while let Some(ch) = scanner.next() {
        if ch.is_alphanumeric() {
            s.push(ch)
        } else if ch == ':' {
            if scanner.expect_str(":KEY") {
                return (Some(s), true)
            } else {
                return (None, false)
            }
        } else if ch.is_whitespace() {
            break
        } else {
            return (None, false)
        }
    }

    assert!(!s.is_empty());
    (Some(s), false)
}

fn read_string(scanner: &mut Scanner) -> Option<Token> {
    let mut s = String::new();
    if scanner.expect('"') {
        while let Some(ch) = scanner.next() {
            if ch == '"' {
                break;
            } else {
                s.push(ch);
            }
        }
    }

    if s.is_empty() {
        None
    } else {
        Some(Token::Symbol(s))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_tokenize() {
        assert_eq!(tokenize("abc def ghi").unwrap(), vec![symbol("abc"), symbol("def"), symbol("ghi")]);
        assert_eq!(tokenize("abc:NUMBER"), None);
        assert_eq!(tokenize("abc::NUMBER").unwrap(), vec![symbol_with_type("abc", "NUMBER")]);
        assert_eq!(tokenize("abc::NUMBER::KEY").unwrap(), vec![symbol_with_key_type("abc", "NUMBER")]);
        assert_eq!(tokenize("abc | def").unwrap(), vec![symbol("abc"), pipe(), symbol("def")]);
        assert_eq!(tokenize("create_table abc::TEXT").unwrap(), vec![symbol("create_table"), symbol_with_type("abc", "TEXT")]);
        assert_eq!(tokenize(r#"a "b cd" e"#).unwrap(), vec![symbol("a"), symbol("b cd"), symbol("e")]);
    }

    fn symbol(s: &str) -> Token {
        Token::Symbol(s.to_string())
    }

    fn symbol_with_type(s: &str, t: &str) -> Token {
        Token::SymbolWithType(s.to_string(), t.to_string())
    }

    fn symbol_with_key_type(s: &str, t: &str) -> Token {
        Token::SymbolWithKeyType(s.to_string(), t.to_string())
    }

    fn pipe() -> Token {
        Token::Pipe
    }
}
