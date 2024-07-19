use std::io::{self, Read, Write};

use crate::page::Page;

#[derive(Default)]
pub(crate) struct Heap {
    pages: Vec<Page>,
}

impl Heap {
    pub(crate) fn read(mut r: impl Read) -> io::Result<Self> {
        let mut pages = Vec::new();
        loop {
            if let Ok(page) = Page::read(&mut r) {
                pages.push(page);
            } else {
                return Ok(Self { pages })
            }
        }
    }

    pub(crate) fn push(&mut self, tuple: &[u8]) {
        if self.pages.is_empty() {
            self.pages.push(Page::new());
        }

        loop {
            let last = self.pages.len() - 1;
            let page = &mut self.pages[last];
            if page.push(tuple).is_ok() {
                break;
            } else {
                self.pages.push(Page::new());
            }
        }
    }

    pub(crate) fn tuples(&self) -> impl Iterator<Item = &[u8]> {
        self.pages.iter().flat_map(Page::tuples)
    }

    #[cfg(test)]
    fn allocated_pages(&self) -> usize {
        self.pages.len()
    }

    pub(crate) fn write(&self, mut w: impl Write) -> io::Result<()> {
        for page in &self.pages {
            page.write(&mut w)?;
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::error::Error;

    use crate::test_util::large_tuple;

    use super::*;

    #[test]
    fn test_write_to_heap() {
        let mut heap = Heap::default();
        let expected_tuple = [0, 0, 0, 1];
        heap.push(&expected_tuple);

        let actual_tuple = heap.tuples().next().unwrap();
        assert_eq!(actual_tuple, expected_tuple);
    }

    #[test]
    fn test_heap_larger_than_page() {
        let tuple = large_tuple::<1024>();

        let mut heap = Heap::default();
        for _ in 0..10 {
            heap.push(&tuple);
        }

        assert!(heap.allocated_pages() > 1);
        assert_eq!(heap.tuples().count(), 10);
    }

    #[test]
    fn test_write_and_read_heap() -> Result<(), Box<dyn Error>> {
        let tuple = large_tuple::<1024>();

        let mut heap = Heap::default();
        for _ in 0..10 {
            heap.push(&tuple);
        }

        let mut buf = Vec::<u8>::new();
        heap.write(&mut buf)?;

        let heap = Heap::read(buf.as_slice())?;
        assert_eq!(heap.tuples().count(), 10);

        Ok(())
    }
}
