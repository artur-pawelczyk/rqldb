use crate::Type;

pub(crate) trait IntoBytes {
    fn kind() -> Type;
    fn iter_bytes(&self) -> impl Iterator<Item = u8>;

    fn to_byte_vec(&self) -> Vec<u8> {
        self.iter_bytes().collect()
    }
}

impl IntoBytes for i32 {
    fn kind() -> Type {
        Type::NUMBER
    }

    fn iter_bytes(&self) -> impl Iterator<Item = u8> {
        self.to_be_bytes().into_iter()
    }
}

impl IntoBytes for &str {
    fn kind() -> Type {
        Type::TEXT
    }

    fn to_byte_vec(&self) -> Vec<u8> {
        let mut v = vec![self.len() as u8];
        v.extend(self.bytes());
        v
    }

    fn iter_bytes(&self) -> impl Iterator<Item = u8> {
        StrBytes(-1, self)
    }
}

struct StrBytes<'a>(i16, &'a str);
impl<'a> Iterator for StrBytes<'a> {
    type Item = u8;

    fn next(&mut self) -> Option<Self::Item> {
        let byte = if self.0 < 0 {
            Some(self.1.len() as u8)
        } else {
            self.1.bytes().nth(self.0 as usize)
        };

        self.0 += 1;
        byte
    }
}

pub(crate) fn into_bytes(kind: Type, s: &str) -> Result<Vec<u8>, ()> {
    match kind {
        Type::NUMBER => s.parse::<i32>().map(|i| i.to_byte_vec()).map_err(|_| ()),
        Type::TEXT => Ok(s.to_byte_vec()),
        _ => todo!(),
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
