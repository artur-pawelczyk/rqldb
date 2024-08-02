use crate::page::{Page, PageMut, RawTuple, PAGE_SIZE};

#[derive(Default)]
pub(crate) struct Heap {
    pages: Vec<u8>,
}

impl Heap {
    pub(crate) fn push(&mut self, tuple: &[u8]) {
        if self.pages.is_empty() {
            self.expand();
        }

        loop {
            let last_page_start = self.pages.len() - PAGE_SIZE;
            let mut page = PageMut::new(&mut self.pages[last_page_start..]);
            if page.push(tuple).is_ok() {
                break;
            } else {
                self.expand();
            }
        }
    }

    fn expand(&mut self) {
        self.pages.extend([0; PAGE_SIZE]);
    }

    pub(crate) fn tuples<'a>(&'a self) -> impl Iterator<Item = RawTuple<'a>> + 'a {
        HeapIter { pages: &self.pages, tuple_n: 0 }
    }

    #[cfg(test)]
    fn allocated_pages(&self) -> usize {
        self.pages.len() / PAGE_SIZE
    }
}

struct HeapIter<'a> {
    pages: &'a [u8],
    tuple_n: usize,
}

impl<'a> Iterator for HeapIter<'a> {
    type Item = RawTuple<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        while self.pages.len() >= PAGE_SIZE {
            if let Some(tuple) = Page::new(&self.pages[..PAGE_SIZE]).nth(self.tuple_n) {
                self.tuple_n += 1;
                return Some(tuple);
            } else {
                self.pages = &self.pages[PAGE_SIZE..];
                self.tuple_n = 0;
            }
        }

        None
    }
}

#[cfg(test)]
mod tests {
    use std::error::Error;

    use super::*;

    #[test]
    fn test_empty_heap() -> Result<(), Box<dyn Error>> {
        let heap = Heap::default();
        assert_eq!(heap.tuples().count(), 0);
        Ok(())
    }

    #[test]
    fn test_write_to_heap() -> Result<(), Box<dyn Error>> {
        let mut heap = Heap::default();
        let expected_tuple = [0, 0, 0, 1];
        heap.push(&expected_tuple);

        let actual_tuple = heap.tuples().next().unwrap();
        assert_eq!(actual_tuple.contents(), expected_tuple);

        Ok(())
    }

    #[test]
    fn test_heap_larger_than_page() -> Result<(), Box<dyn Error>> {
        let tuple = large_tuple::<1024>();

        let mut heap = Heap::default();
        for _ in 0..10 {
            heap.push(&tuple);
        }

        assert!(heap.allocated_pages() > 1);
        assert_eq!(heap.tuples().count(), 10);

        Ok(())
    }

    pub(crate) fn large_tuple<const N: usize>() -> [u8; N] {
        let mut v = [0; N];
        v[0] = 123;
        v
    }
}
