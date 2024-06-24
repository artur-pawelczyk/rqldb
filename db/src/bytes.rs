use std::io::Write;

use crate::Type;

pub(crate) trait IntoBytes {
    fn kind() -> Type;
    fn write_bytes<W: Write>(&self, w: &mut W) -> Result<(), ()>;

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

    fn write_bytes<W: Write>(&self, w: &mut W) -> Result<(), ()> {
        w.write_all(&self.to_be_bytes()).map_err(|_| ())
    }
}

impl IntoBytes for &str {
    fn kind() -> Type {
        Type::TEXT
    }

    fn write_bytes<W: Write>(&self, w: &mut W) -> Result<(), ()> {
        w.write_all(&[self.len() as u8]).map_err(|_| ())?;
        w.write_all(self.as_bytes()).map_err(|_| ())?;
        Ok(())
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

    fn write_bytes<W: Write>(&self, w: &mut W) -> Result<(), ()> {
        if *self {
            w.write_all(&[1]).map_err(|_| ())
        } else {
            w.write_all(&[0]).map_err(|_| ())
        }
    }
}

pub(crate) fn into_bytes(kind: Type, s: &str) -> Result<Vec<u8>, ()> {
    let mut v = Vec::new();
    write_as_bytes(kind, s, &mut v)?;
    Ok(v)
}

pub(crate) fn write_as_bytes<W: Write>(kind: Type, s: &str, w: &mut W) -> Result<(), ()> {
    match kind {
        Type::TEXT => {
            s.write_bytes(w)
        },
        Type::NUMBER => {
            let i =  s.parse::<i32>().map_err(|_| ())?;
            i.write_bytes(w)
        },
        Type::BOOLEAN => {
            let b = s.parse::<bool>().map_err(|_| ())?;
            b.write_bytes(w)
        },
        _ => todo!("Not implemented for {kind}"),
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

        assert_eq!(into_bytes(Type::NUMBER, "not-number"), Err(()));
    }
}
