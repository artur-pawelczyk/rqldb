use core::fmt;
use std::{error::Error, io::{self, Write}};

use crate::Type;

pub(crate) trait IntoBytes {
    fn kind() -> Type;
    fn write_bytes<W: Write>(&self, w: &mut W) -> Result<(), io::Error>;

    fn to_byte_vec(&self) -> Vec<u8> {
        let mut v = Vec::new();
        self.write_bytes(&mut v).expect("Will never fail for a vec");
        v
    }
}

impl IntoBytes for i32 {
    fn kind() -> Type {
        Type::NUMBER
    }

    fn write_bytes<W: Write>(&self, w: &mut W) -> Result<(), io::Error> {
        w.write_all(&self.to_be_bytes())
    }
}

impl IntoBytes for &str {
    fn kind() -> Type {
        Type::TEXT
    }

    fn write_bytes<W: Write>(&self, w: &mut W) -> Result<(), io::Error> {
        w.write_all(&[self.len() as u8])?;
        w.write_all(self.as_bytes())
    }

    fn to_byte_vec(&self) -> Vec<u8> {
        let mut v = vec![self.len() as u8];
        v.extend(self.bytes());
        v
    }
}

impl IntoBytes for bool {
    fn kind() -> Type {
        Type::TEXT
    }

    fn write_bytes<W: Write>(&self, w: &mut W) -> Result<(), io::Error> {
        if *self {
            w.write_all(&[1])
        } else {
            w.write_all(&[0])
        }
    }
}

pub(crate) fn into_bytes(kind: Type, s: &str) -> Result<Vec<u8>, Box<dyn Error>> {
    let mut v = Vec::new();
    write_as_bytes(kind, s, &mut v)?;
    Ok(v)
}

pub(crate) fn write_as_bytes<W: Write>(kind: Type, s: &str, w: &mut W) -> Result<(), Box<dyn Error>> {
    match kind {
        Type::TEXT => {
            Ok(s.write_bytes(w)?)
        },
        Type::NUMBER => {
            let i =  s.parse::<i32>()
                .map_err(|_| EncodingError::ExpectedNumber(Box::from(s)))?;
            Ok(i.write_bytes(w)?)
        },
        Type::BOOLEAN => {
            let b = s.parse::<bool>()
                .map_err(|_| EncodingError::ExpectedBoolean(Box::from(s)))?;
            Ok(b.write_bytes(w)?)
        },
        _ => todo!("Not implemented for {kind}"),
    }
}


#[derive(Debug)]
enum EncodingError {
    ExpectedNumber(Box<str>),
    ExpectedBoolean(Box<str>),
}

impl fmt::Display for EncodingError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ExpectedNumber(s) => write!(f, "Expected a number, got {s}"),
            Self::ExpectedBoolean(s) => write!(f, "Expected a boolean, got {s}"),
        }
    }
}

impl Error for EncodingError {}

impl Type {
    pub(crate) fn size(&self, bytes: &[u8]) -> usize {
        match self {
            Type::NUMBER => 4,
            Type::TEXT => bytes[0] as usize + 1,
            Type::BOOLEAN => 1,
            _ => todo!("Not implemented for {self}")
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_i32_to_bytes() {
        assert_eq!(0i32.to_byte_vec(), vec![0, 0, 0, 0]);
        assert_eq!(1i32.to_byte_vec(), vec![0, 0, 0, 1]);
    }

    #[test]
    fn test_str_to_bytes() {
        assert_eq!("abc".to_byte_vec(), vec![3, 97, 98, 99]);
    }

    #[test]
    fn test_str_and_type_to_bytes() {
        assert_eq!(into_bytes(Type::TEXT, "abc").unwrap(), vec![3, 97, 98, 99]);
        assert_eq!(into_bytes(Type::NUMBER, "123").unwrap(), vec![0, 0, 0, 123]);

        assert!(matches!(into_bytes(Type::NUMBER, "not-number"), Err(_)));
    }

    #[test]
    fn test_tuple_element_length() {
        let mut bytes = into_bytes(Type::NUMBER, "123").unwrap();
        bytes.extend([1, 2, 3, 4]);
        assert_eq!(Type::NUMBER.size(&bytes), 4);

        let mut bytes = into_bytes(Type::TEXT, "abcd").unwrap();
        bytes.extend([1, 2, 3, 4]);
        assert_eq!(Type::TEXT.size(&bytes), 5);
    }
}
