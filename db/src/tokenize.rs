use std::fmt;

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Token<'a> {
    Symbol(&'a str, usize),
    SymbolWithType(&'a str, &'a str, usize),
    SymbolWithKeyType(&'a str, &'a str, usize),
    Pipe(usize),
}

pub struct Tokenizer<'a> {
    source: &'a str,
    pos: usize,
}

impl<'a> Tokenizer<'a> {
    pub fn new(source: &'a str) -> Self {
        Self{ source, pos: 0 }
    }
}

impl<'a> Iterator for Tokenizer<'a> {
    type Item = Token<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        for (pos, ch) in self.source.char_indices().skip(self.pos) {
            if ch == '|' {
                self.pos += 1;
                return Some(Token::Pipe(pos))
            } else if ch == '"' {
                if let Some(s) = read_string(&self.source[pos..], pos) {
                    self.pos += s.len() + 2;
                    return Some(s)
                }
            } else if ch.is_whitespace() {
                self.pos += 1;
            } else if let Some(sym) = read_symbol(&self.source[pos..], pos) {
                    self.pos += sym.len();
                    return Some(sym)
                }
        }

        None
    }
}

fn read_symbol(source: &str, offset: usize) -> Option<Token<'_>> {
    let part = read_util_whitespace(source);
    let mut chars = part.char_indices();

    while let Some((pos, ch)) = chars.next() {
        if ch == ':' {
            let name_end = pos;
            if chars.next().map(|(_, ch)| ch) == Some(':') {
                while let Some((pos, ch)) = chars.next() {
                    if ch == ':' {
                        let type_end = pos;
                        if chars.next().map(|(_, ch)| ch) == Some(':') {
                            return Some(Token::SymbolWithKeyType(&part[0..name_end], &part[name_end+2..type_end], offset));
                        }
                    }
                }

                return Some(Token::SymbolWithType(&part[0..name_end], &part[name_end+2..], offset))
            }
        }
    }

    if part.is_empty() {
        None
    } else {
        Some(Token::Symbol(part, offset))
    }
}

fn read_string(source: &str, offset: usize) -> Option<Token<'_>> {
    if !source.starts_with('"') {
        return None
    }
    
    for (pos, ch) in source.char_indices().skip(1) {
        if ch == '"' {
            return Some(Token::Symbol(&source[1..pos], offset))
        }
    }

    None
}

fn read_util_whitespace(source: &str) -> &str {
    for (pos, ch) in source.char_indices() {
        if ch.is_whitespace() {
            return &source[0..pos];
        }
    }

    source
}

impl<'a> Token<'a> {
    fn len(&self) -> usize {
        match self {
            Token::Symbol(x, _) => x.len(),
            Token::SymbolWithType(x, y, _) => x.len() + "::".len() + y.len(),
            Token::SymbolWithKeyType(x, y, _) => x.len() + "::".len() + y.len() + "::KEY".len(),
            Token::Pipe(_) => "|".len(),
        }
    }

    pub(crate) fn pos(&self) -> usize {
        match self {
            Token::Symbol(_, pos) => *pos,
            Token::SymbolWithType(_, _, pos) => *pos,
            Token::SymbolWithKeyType(_, _, pos) => *pos,
            Token::Pipe(pos) => *pos,
        }
    }
}

impl<'a> fmt::Display for Token<'a> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Token::Symbol(x, _) => write!(f, "{}", x),
            Token::SymbolWithType(x, y, _) => write!(f, "{}::{}", x, y),
            Token::SymbolWithKeyType(x, y, _) => write!(f, "{}::{}::KEY", x, y),
            Token::Pipe(_) => write!(f, "|"),
        }        
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    macro_rules! symbol {
        ($name:literal) => {
            Token::Symbol($name, _)
        };
        ($name:literal, $pos:literal) => {
            Token::Symbol($name, $pos)
        };
    }

    macro_rules! symbol_with_type {
        ($name:literal, $type:literal) => {
            Token::SymbolWithType($name, $type, _)
        };
        ($name:literal, $type:literal, $pos:literal) => {
            Token::SymbolWithType($name, $type, $pos)
        };
    }

    macro_rules! symbol_with_key_type {
        ($name:literal, $type:literal) => {
            Token::SymbolWithKeyType($name, $type, _)
        };
        ($name:literal, $type:literal, $pos:literal) => {
            Token::SymbolWithKeyType($name, $type, $pos)
        };
    }

    macro_rules! pipe {
        () => {
            Token::Pipe(_)
        };
        ($pos:literal) => {
            Token::Pipe($pos)
        };
    }

    macro_rules! assert_tokenize {
        ($source:literal, $($token:pat),*) => {
            let result = tokenize($source).unwrap();
            if !matches!(result[..], [$($token, )*]) {
                dbg!(&result);
                assert!(matches!(result[..], [$($token, )*]));
            }
        }
    }

    #[test]
    fn test_tokenize() {
        assert_tokenize!("abc def ghi", symbol!("abc", 0), symbol!("def", 4), symbol!("ghi", 8));
        assert_tokenize!("abc::NUMBER", symbol_with_type!("abc", "NUMBER"));
        assert_tokenize!("abc:NUMBER", symbol!("abc:NUMBER"));
        assert_tokenize!("abc::NUMBER::KEY", symbol_with_key_type!("abc", "NUMBER"));
        assert_tokenize!("abc | def", symbol!("abc", 0), pipe!(4), symbol!("def", 6));
        assert_tokenize!("create_table abc::TEXT", symbol!("create_table", 0), symbol_with_type!("abc", "TEXT", 13));
        assert_tokenize!("document.id document.name", symbol!("document.id", 0), symbol!("document.name", 12));
        assert_tokenize!(r#"a "b cd" e"#, symbol!("a", 0), symbol!("b cd", 2), symbol!("e", 9));
    }

    fn tokenize(source: &str) -> Option<Vec<Token<'_>>> {
        let tokenizer = Tokenizer::new(source);
        let mut v = Vec::new();
        for t in tokenizer {
            v.push(t);
        }

        Some(v)
    }
}
