use std::collections::HashMap;
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};

pub trait IdMap<T>
where T: Eq + Hash
{
    fn get(&self, id: usize) -> Option<&T>;
    fn lookup_value(&self, val: &T) -> Option<usize>;
    fn iter(&self) -> IdMapIter<T>;
    fn insert(&mut self, val: T) -> InsertResult;
}

#[derive(PartialEq, Debug)]
pub enum InsertResult {
    Added(usize),
    Replaced(usize),
}

#[allow(dead_code)]
impl InsertResult {
    pub(crate) fn id(&self) -> usize {
        match self {
            Self::Added(id) => *id,
            Self::Replaced(id) => *id,
        }
    }
}

pub(crate) struct HashIdMap<T, K>
where T: Eq + Hash,
      K: Eq + Hash,
{
    inner: Vec<T>,
    hash: HashMap<u64, Vec<usize>>,
    key_extractor: Box<dyn KeyExtractor<T, K>>,
}

impl<T: Eq + Hash> HashIdMap<T, T> {
    pub(crate) fn new() -> Self {
        Self {
            inner: Vec::new(),
            hash: HashMap::new(),
            key_extractor: Box::new(IdentityExtractor),
        }
    }

    pub(crate) fn from(source: Vec<T>) -> Self {
        let mut map = Self::new();
        for (id, val) in source.into_iter().enumerate() {
            let h = hash(&val);
            map.inner.insert(id, val);
            map.hash.entry(h).and_modify(|v| v.push(id)).or_insert_with(|| vec![id]);
        }

        map
    }
}

#[allow(dead_code)]
impl<T, K> HashIdMap<T, K>
where T: Eq + Hash,
      K: Eq + Hash,
{
    pub(crate) fn with_key_extractor(e: impl KeyExtractor<T, K> + 'static) -> Self {
        Self{
            inner: Vec::new(),
            hash: HashMap::new(),
            key_extractor: Box::new(e),
        }
    }

    pub(crate) fn from_with_key_extractor(source: Vec<T>, e: impl KeyExtractor<T, K> + 'static) -> Self {
        let mut map = Self::with_key_extractor(e);
        for (id, val) in source.into_iter().enumerate() {
            let h = hash(&val);
            map.inner.insert(id, val);
            map.hash.entry(h).and_modify(|v| v.push(id)).or_insert_with(|| vec![id]);
        }

        map
    }

    fn find_id(&self, val: &T) -> Option<usize> {
        let key = self.extract_key(val);
        let key_hash = hash(key);
        self.hash.get(&key_hash)?.iter().find(|id| {
            if let Some(v) = self.inner.get(**id) {
                key == self.extract_key(v)
            } else {
                false
            }
        }).map(|x| *x)
    }

    fn extract_key<'a>(&self, val: &'a T) -> &'a K {
        self.key_extractor.extract(val)
    }

    pub fn len(&self) -> usize {
        self.inner.len()
    }
}

impl<T, K> IdMap<T> for HashIdMap<T, K> 
    where T: Eq + Hash,
          K: Eq + Hash,
{
    fn insert(&mut self, val: T) -> InsertResult {
        if let Some(id) = self.find_id(&val) {
            let _ = std::mem::replace(&mut self.inner[id], val);
            InsertResult::Replaced(id)
        } else {
            let id = self.inner.len();
            let h = hash(&self.extract_key(&val));
            self.inner.insert(id, val);
            self.hash.entry(h).and_modify(|v| v.push(id)).or_insert_with(|| vec![id]);
            InsertResult::Added(id)
        }
    }

    fn get(&self, id: usize) -> Option<&T> {
        self.inner.get(id)
    }

    fn lookup_value(&self, val: &T) -> Option<usize> {
        self.hash.get(&hash(&val))
            .and_then(|v| {
                v.iter().find(|id| {
                    let a = unsafe {
                        self.inner.get_unchecked(**id)
                    };

                    a == val
                })
            })
            .map(|x| *x)
    }

    fn iter(&self) -> IdMapIter<T> {
        IdMapIter{
            inner: self.inner.iter(),
        }
    }
}

pub struct IdMapIter<'a, T>
where T: Eq + Hash,
{
    inner: std::slice::Iter<'a, T>,
}

#[allow(dead_code)]
impl<'a, T> Iterator for IdMapIter<'a, T>
where T: Eq + Hash,
{
    type Item = &'a T;

    fn next(&mut self) -> Option<Self::Item> {
        self.inner.next()
    }
}

pub(crate) trait KeyExtractor<A, B> {
    fn extract<'a>(&self, x: &'a A) -> &'a B;
}

struct IdentityExtractor;
impl<T> KeyExtractor<T, T> for IdentityExtractor {
    fn extract<'a>(&self, x: &'a T) -> &'a T {
        x
    }
}

fn hash<T: Hash>(x: &T) -> u64 {
    let mut hasher = DefaultHasher::default();
    x.hash(&mut hasher);
    hasher.finish()
}

#[cfg(test)]
mod tests {
    use std::marker::PhantomData;

    use super::*;

    #[test]
    fn test_insert_and_get() {
        let mut map = HashIdMap::new();
        let id = map.insert("one").id();

        assert_eq!(map.get(id), Some(&"one"));
        assert_eq!(map.lookup_value(&"one"), Some(id));
    }

    #[test]
    fn test_insert_same_value() {
        let mut map = HashIdMap::new();

        map.insert("one");
        let id = map.insert("two").id();
        assert_eq!(map.insert("two"), InsertResult::Replaced(id));
    }

    #[test]
    fn test_from_existing_values() {
        let mut map = HashIdMap::from(vec!["a", "b", "c"]);

        assert_eq!(map.get(0), Some(&"a"));
        assert_eq!(map.lookup_value(&"b"), Some(1));
        assert_eq!(map.insert("c"), InsertResult::Replaced(2));
    }

    #[test]
    fn test_iter() {
        let source: Vec<String> = ["a", "b", "c"].iter().map(|x| x.to_string()).collect();
        let map = HashIdMap::from(source.clone());

        assert_eq!(map.iter().cloned().collect::<Vec<String>>(), source);
    }

    #[test]
    fn test_key_extractor() {
        let mut map: HashIdMap<MockStruct, usize> = HashIdMap::with_key_extractor(MockKeyExtractor{ marker: PhantomData });
        let s1 = MockStruct{ id: 1, t: "a" };
        let s2 = MockStruct{ id: 1, t: "b" };

        assert_eq!(map.insert(s1), InsertResult::Added(0));
        assert_eq!(map.insert(s2), InsertResult::Replaced(0));
        assert_eq!(map.get(0), Some(&MockStruct{ id: 1, t: "b" }));
    }

    struct MockKeyExtractor<'a> {
        marker: PhantomData<&'a ()>,
    }
    impl<'a> KeyExtractor<MockStruct<'a>, usize> for MockKeyExtractor<'a> {
        fn extract<'k>(&self, x: &'k MockStruct) -> &'k usize {
            &x.id
        }
    }

    #[derive(Debug, PartialEq, Eq, Hash)]
    struct MockStruct<'a> {
        id: usize,
        t: &'a str,
    }
}
