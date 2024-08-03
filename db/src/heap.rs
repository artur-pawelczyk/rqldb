use crate::page::{Page, PageMut, RawTuple, TupleId, PAGE_SIZE};

#[derive(Default)]
pub(crate) struct Heap {
    pages: Vec<u8>,
}

impl Heap {
    pub(crate) fn push(&mut self, tuple: &[u8]) -> TupleId {
        if self.pages.is_empty() {
            self.expand();
        }

        loop {
            let last_page_start = self.pages.len() - PAGE_SIZE;
            let mut page = PageMut::new(self.allocated_pages() as u32 - 1, &mut self.pages[last_page_start..]);
            if let Ok(id) = page.push(tuple) {
                return id;
            } else {
                self.expand();
            }
        }
    }

    pub(crate) fn tuple_by_id<'a>(&'a self, id: TupleId) -> RawTuple<'a> {
        let page_start = id.block as usize * PAGE_SIZE;
        let page_end = page_start + PAGE_SIZE;
        Page::new(id.block, &self.pages[page_start..page_end]).tuple_by_id(id)
    }

    pub(crate) fn delete(&mut self, id: TupleId) {
        let page_start = id.block as usize * PAGE_SIZE;
        let page_end = page_start + PAGE_SIZE;
        PageMut::new(self.allocated_pages() as u32 - 1, &mut self.pages[page_start..page_end])
            .delete(id);
    }

    fn expand(&mut self) {
        self.pages.extend([0; PAGE_SIZE]);
    }

    pub(crate) fn tuples<'a>(&'a self) -> impl Iterator<Item = RawTuple<'a>> + 'a {
        self.pages().flatten()
    }

    fn pages<'a>(&'a self) -> impl Iterator<Item = Page<'a>> {
        PageIter(0, &self.pages)
    }

    fn allocated_pages(&self) -> usize {
        self.pages.len() / PAGE_SIZE
    }
}

struct PageIter<'a>(u32, &'a [u8]);

impl<'a> Iterator for PageIter<'a> {
    type Item = Page<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.1.len() >= PAGE_SIZE {
            let page = Page::new(self.0, &self.1[..PAGE_SIZE]);
            self.0 += 1;
            self.1 = &self.1[PAGE_SIZE..];
            Some(page)
        } else {
            None
        }
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
        let mut heap = Heap::default();
        let ids = (1..=10)
            .map(|i| heap.push(&large_tuple::<1024>(i)))
            .collect::<Vec<_>>();

        assert!(heap.allocated_pages() > 1);
        assert_eq!(heap.tuples().count(), 10);

        let first = heap.tuple_by_id(ids[0]).contents();
        let last = heap.tuple_by_id(ids[9]).contents();
        assert_eq!(first[0], 1);
        assert_eq!(last[0], 10);

        Ok(())
    }

    #[test]
    fn test_delete_tuple_from_heap() -> Result<(), Box<dyn Error>> {
        let mut heap = Heap::default();

        let ids = (0..10)
            .map(|i| heap.push(&large_tuple::<1024>(i)))
            .collect::<Vec<_>>();
        assert_eq!(heap.tuples().count(), 10);

        heap.delete(ids[0]);
        let ids_after = heap.tuples().map(|t| t.id()).collect::<Vec<_>>();
        assert_eq!(ids_after[..], ids[1..]);

        Ok(())
    }

    pub(crate) fn large_tuple<const N: usize>(value: u8) -> [u8; N] {
        let mut v = [0; N];
        v[0] = value;
        v
    }
}
