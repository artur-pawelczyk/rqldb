#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Token<'a> {
    Symbol(&'a str),
    SymbolWithType(&'a str, &'a str),
    SymbolWithKeyType(&'a str, &'a str),
    Pipe,
}

pub struct Tokenizer<'a> {
    source: &'a str,
}

impl<'a> Tokenizer<'a> {
    pub fn from_str(source: &'a str) -> Self {
        Self{ source }
    }

    pub fn next_2(&mut self) -> Option<Token> {
        todo!()
    }
}

impl<'a> Iterator for Tokenizer<'a> {
    type Item = Token<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        for ch in self.source.chars() {
            if ch == '|' {
                self.source = &self.source[1..];
                return Some(Token::Pipe)
            } else if ch == '"' {
                if let Some(s) = read_string(self.source) {
                    self.source = &self.source[s.len()+2..];
                    return Some(s)
                }
            } else if ch.is_whitespace() {
                self.source = &self.source[1..];
            } else {
                if let Some(sym) = read_symbol(self.source) {
                    self.source = &self.source[sym.len()..];
                    return Some(sym)
                }
            }
        }

        None
    }
}

fn read_symbol(source: &str) -> Option<Token<'_>> {
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
                            return Some(Token::SymbolWithKeyType(&part[0..name_end], &part[name_end+2..type_end]));
                        }
                    }
                }

                return Some(Token::SymbolWithType(&part[0..name_end], &part[name_end+2..]))
            }
        }
    }

    if part.is_empty() {
        None
    } else {
        Some(Token::Symbol(part))
    }
}

fn read_string(source: &str) -> Option<Token<'_>> {
    if !source.starts_with('"') {
        return None
    }
    
    for (pos, ch) in source.char_indices().skip(1) {
        if ch == '"' {
            return Some(Token::Symbol(&source[1..pos]))
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
            Token::Symbol(x) => x.len(),
            Token::SymbolWithType(x, y) => x.len() + "::".len() + y.len(),
            Token::SymbolWithKeyType(x, y) => x.len() + "::".len() + y.len() + "::KEY".len(),
            Token::Pipe => "|".len(),
        }
    }

    pub fn to_string(&self) -> String {
        match self {
            Token::Symbol(x) => x.to_string(),
            Token::SymbolWithType(x, y) => x.to_string() + "::" + y,
            Token::SymbolWithKeyType(x, y) => x.to_string() + "::" + y + "::KEY",
            Token::Pipe => String::from("|"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_tokenize() {
        assert_eq!(tokenize("abc def ghi").unwrap(), vec![symbol("abc"), symbol("def"), symbol("ghi")]);
        assert_eq!(tokenize("abc::NUMBER").unwrap(), vec![symbol_with_type("abc", "NUMBER")]);
        assert_eq!(tokenize("abc:NUMBER").unwrap(), vec![symbol("abc:NUMBER")]);
        assert_eq!(tokenize("abc::NUMBER::KEY").unwrap(), vec![symbol_with_key_type("abc", "NUMBER")]);
        assert_eq!(tokenize("abc | def").unwrap(), vec![symbol("abc"), pipe(), symbol("def")]);
        assert_eq!(tokenize("create_table abc::TEXT").unwrap(), vec![symbol("create_table"), symbol_with_type("abc", "TEXT")]);
        assert_eq!(tokenize("document.id document.name").unwrap(), vec![symbol("document.id"), symbol("document.name")]);
        assert_eq!(tokenize(r#"a "b cd" e"#).unwrap(), vec![symbol("a"), symbol("b cd"), symbol("e")]);
    }

    fn tokenize<'a>(source: &'a str) -> Option<Vec<Token<'a>>> {
        let tokenizer = Tokenizer::from_str(source);
        let mut v = Vec::new();
        for t in tokenizer {
            v.push(t);
        }

        Some(v)
    }

    fn symbol(s: &str) -> Token<'_> {
        Token::Symbol(s)
    }

    fn symbol_with_type<'a>(s: &'a str, t: &'a str) -> Token<'a> {
        Token::SymbolWithType(s, t)
    }

    fn symbol_with_key_type<'a>(s: &'a str, t: &'a str) -> Token<'a> {
        Token::SymbolWithKeyType(s, t)
    }

    fn pipe<'a>() -> Token<'a> {
        Token::Pipe
    }
}
