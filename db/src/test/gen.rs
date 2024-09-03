use std::cell::Cell;

use crate::Type;

pub(crate) enum Generator {
    Id(Cell<u32>),
    Letter(Box<str>, Cell<usize>),
    Text(Cell<u32>),
    Number(Cell<u32>),
    Cycle(u32, Cell<u32>),
}

impl Generator {
    pub(crate) fn id() -> Self {
        Self::Id(Cell::new(1))
    }

    pub(crate) fn text() -> Self {
        Self::Text(Cell::new(1))
    }

    pub(crate) fn number_from(n: u32) -> Self {
        Self::Number(Cell::new(n))
    }

    pub(crate) fn cycle(n: u32) -> Self {
        Self::Cycle(n, Cell::new(1))
    }

    pub(crate) fn letter(prefix: &str) -> Self {
        Generator::Letter(Box::from(prefix), Cell::new(0))
    }

    pub(crate) fn kind(&self) -> Type {
        match self {
            Generator::Id(_) => Type::NUMBER,
            Generator::Letter(_, _) => Type::TEXT,
            Generator::Text(_) => Type::TEXT,
            Generator::Number(_) => Type::NUMBER,
            Generator::Cycle(_, _) => Type::NUMBER,
        }
    }

    pub(crate) fn next(&self) -> String {
        let out = match self {
            Generator::Id(id) => format!("{}", id.get()),
            Generator::Letter(prefix, ch_pos) => {
                let letter = LETTERS.chars().nth(ch_pos.get()).unwrap();
                format!("{prefix}{letter}")
            },
            Generator::Text(i) => format!("example {}", i.get()),
            Generator::Number(i) => format!("{}", i.get()),
            Generator::Cycle(_, i) => format!("{}", i.get()),
        };

        self.advance();
        out
    }

    fn advance(&self) {
        match self {
            Generator::Id(id) => {
                id.set(id.get() + 1);
            },
            Generator::Letter(_, ch) => {
                ch.set(ch.get() + 1);
                if ch.get() >= LETTERS.len() {
                    ch.set(0);
                }
            },
            Generator::Text(i) => {
                i.set(i.get() + 1);
              },
            Generator::Number(i) => {
                i.set(i.get() + 1);
            },
              Generator::Cycle(max, curr) => {
                  curr.set(curr.get() + 1);
                  if curr.get() > *max {
                      curr.set(1);
                }
            },
        }
    }
}

const LETTERS: &'static str = "abcdefgh";
