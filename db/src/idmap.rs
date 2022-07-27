use std::collections::HashMap;
use std::rc::Rc;
use std::hash::Hash;

pub(crate) struct IdMap<T: Eq + Hash> {
    inner: Vec<Rc<T>>,
    hash: HashMap<Rc<T>, usize>,
}

#[allow(dead_code)]
impl<T: Eq + Hash> IdMap<T> {
    pub(crate) fn new() -> Self {
        Self{
            inner: Vec::new(),
            hash: HashMap::new(),
        }
    }

    pub(crate) fn from(source: Vec<T>) -> Self {
        let mut map = Self::new();
        for (id, val) in source.into_iter().enumerate() {
            let r = Rc::new(val);
            map.hash.insert(Rc::clone(&r), id);
            map.inner.push(r);
        }

        map
    }

    pub(crate) fn insert(&mut self, val: T) -> InsertResult {
        if let Some(id) = self.hash.get(&val) {
            InsertResult::Ignored(*id)
        } else {
            let id = self.inner.len();
            let r = Rc::new(val);
            self.hash.insert(Rc::clone(&r), id);
            self.inner.push(r);
            InsertResult::Added(id)
        }
    }

    pub(crate) fn get(&self, id: usize) -> Option<&T> {
        self.inner.get(id).map(|x| x.as_ref())
    }

    pub(crate) fn lookup_value(&self, val: &T) -> Option<usize> {
        self.hash.get(val).copied()
    }

    pub(crate) fn iter<'a>(&'a self) -> IdMapIter<'a, T> {
        IdMapIter{
            inner: self.inner.iter(),
        }
    }
}

pub(crate) struct IdMapIter<'a, T>
where T: Eq + Hash,
{
    inner: std::slice::Iter<'a, Rc<T>>,
}

#[allow(dead_code)]
impl<'a, T> Iterator for IdMapIter<'a, T>
where T: Eq + Hash,
{
    type Item = &'a T;

    fn next(&mut self) -> Option<Self::Item> {
        self.inner.next().map(|x| x.as_ref())
    }
}

#[derive(PartialEq, Debug)]
pub(crate) enum InsertResult {
    Added(usize),
    Ignored(usize),
}

#[allow(dead_code)]
impl InsertResult {
    pub(crate) fn id(&self) -> usize {
        match self {
            Self::Added(id) => *id,
            Self::Ignored(id) => *id,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_insert_and_get() {
        let mut map = IdMap::new();
        let id = map.insert("one").id();

        assert_eq!(map.get(id), Some(&"one"));
        assert_eq!(map.lookup_value(&"one"), Some(id));
    }

    #[test]
    fn test_insert_same_value() {
        let mut map = IdMap::new();

        map.insert("one");
        let id = map.insert("two").id();
        assert_eq!(map.insert("two"), InsertResult::Ignored(id));
    }

    #[test]
    fn test_from_existing_values() {
        let mut map = IdMap::from(vec!["a", "b", "c"]);

        assert_eq!(map.get(0), Some(&"a"));
        assert_eq!(map.lookup_value(&"b"), Some(1));
        assert_eq!(map.insert("c"), InsertResult::Ignored(2));
    }

    #[test]
    fn test_iter() {
        let source: Vec<String> = ["a", "b", "c"].iter().map(|x| x.to_string()).collect();
        let map = IdMap::from(source.clone());

        assert_eq!(map.iter().cloned().collect::<Vec<String>>(), source);
    }
}
