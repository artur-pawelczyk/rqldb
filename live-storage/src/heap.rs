use std::{fs::File, io::{self, Cursor, Read, Write as _}, path::Path};

use crate::page::{Page, PageMut, PAGE_SIZE};

pub(crate) struct Heap {
    pages: Vec<u8>,
    file: File,
}

impl Heap {
    pub(crate) fn open(path: &Path) -> io::Result<Self> {
        let h = Self {
            pages: Vec::new(),
            file: File::options().read(true).write(true).create(true).open(path)?,
        };

        h.read(File::open(path)?)
    }

    pub(crate) fn read(self, mut r: impl Read) -> io::Result<Self> {
        let mut pages = Vec::new();
        r.read_to_end(&mut pages)?;
        Ok(Self { pages, ..self })
    }

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

    pub(crate) fn tuples(&self) -> impl Iterator<Item = Vec<u8>> + '_ {
        HeapIter { pages: &self.pages, tuple_n: 0 }
    }

    #[cfg(test)]
    fn allocated_pages(&self) -> usize {
        self.pages.len() / PAGE_SIZE
    }

    pub(crate) fn write(&mut self) -> io::Result<()> {
        self.file.write_all(&self.pages)
    }
}

struct HeapIter<'a> {
    pages: &'a [u8],
    tuple_n: usize,
}

impl<'a> Iterator for HeapIter<'a> {
    type Item = Vec<u8>;

    fn next(&mut self) -> Option<Self::Item> {
        while self.pages.len() >= PAGE_SIZE {
            if let Some(tuple) = Page::new(Cursor::new(&self.pages[..PAGE_SIZE])).nth(self.tuple_n) {
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
    use std::{error::Error, path::PathBuf};

    use crate::test_util::large_tuple;

    use super::*;

    #[test]
    fn test_empty_heap() -> Result<(), Box<dyn Error>> {
        let heap = Heap::open(&temp_heap_file()?)?;
        assert_eq!(heap.tuples().count(), 0);
        Ok(())
    }

    #[test]
    fn test_write_to_heap() -> Result<(), Box<dyn Error>> {
        let mut heap = Heap::open(&temp_heap_file()?)?;
        let expected_tuple = [0, 0, 0, 1];
        heap.push(&expected_tuple);

        let actual_tuple = heap.tuples().next().unwrap();
        assert_eq!(actual_tuple, expected_tuple);

        Ok(())
    }

    #[test]
    fn test_heap_larger_than_page() -> Result<(), Box<dyn Error>> {
        let tuple = large_tuple::<1024>();

        let mut heap = Heap::open(&temp_heap_file()?)?;
        for _ in 0..10 {
            heap.push(&tuple);
        }

        assert!(heap.allocated_pages() > 1);
        assert_eq!(heap.tuples().count(), 10);

        Ok(())
    }

    #[test]
    fn test_write_and_read_heap() -> Result<(), Box<dyn Error>> {
        let tuple = large_tuple::<1024>();
        let heap_file = temp_heap_file()?;

        let mut heap = Heap::open(&heap_file)?;
        for _ in 0..10 {
            heap.push(&tuple);
        }

        heap.write()?;

        let heap = Heap::open(&heap_file)?;
        assert_eq!(heap.tuples().count(), 10);

        Ok(())
    }

    fn temp_heap_file() -> io::Result<PathBuf> {
        let mut heap_file = tempfile::tempdir()?.into_path();
        heap_file.push("object");
        Ok(heap_file)
    }
}
