use std::{fs::File, io::{self, Read, Seek, SeekFrom, Write}, path::Path};

use crate::page::{Page, PAGE_SIZE};
use rqldb::page::BlockId;

pub(crate) struct Heap {
    file: File,
}

impl Heap {
    pub(crate) fn open(path: &Path) -> io::Result<Self> {
        Ok(Self {
            file: File::options().read(true).write(true).create(true).open(path)?,
        })
    }

    pub(crate) fn write_page(&mut self, block: BlockId, b: &[u8]) -> io::Result<()> {
        debug_assert!(b.len() == PAGE_SIZE);

        self.file.seek(SeekFrom::Start(block as u64 * PAGE_SIZE as u64))?;
        self.file.write_all(b)?;

        Ok(())
    }

    // TODO: Remove
    pub(crate) fn tuples<'a>(&'a self) -> impl Iterator<Item = Vec<u8>> + 'a {
        HeapIter { file: &self.file, tuple_n: 0, buf: [0u8; PAGE_SIZE] }
    }
}

struct HeapIter<'a> {
    file: &'a File,
    buf: [u8; PAGE_SIZE],
    tuple_n: usize,
}

impl<'a> Iterator for HeapIter<'a> {
    type Item = Vec<u8>;

    fn next(&mut self) -> Option<Self::Item> {

        loop {
            if self.tuple_n == 0 {
                self.file.read_exact(&mut self.buf).ok()?;
            }

            if let Some(tuple) = Page::new(&self.buf).nth(self.tuple_n) {
                self.tuple_n += 1;
                return Some(tuple.to_vec()); // TODO: Avoid this allocation
            } else {
                self.tuple_n = 0;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{error::Error, path::PathBuf};

    use super::*;

    #[test]
    fn test_empty_heap() -> Result<(), Box<dyn Error>> {
        let heap = Heap::open(&temp_heap_file()?)?;
        assert_eq!(heap.tuples().count(), 0);
        Ok(())
    }

    #[test]
    fn test_write_heap() -> Result<(), Box<dyn Error>> {
        let file = temp_heap_file()?;
        let mut heap = Heap::open(&file)?;

        heap.write_page(0, &page_contents(1))?;

        assert_eq!(File::open(&file)?.metadata()?.len(), PAGE_SIZE as u64);

        Ok(())
    }

    #[test]
    fn test_extend_heap() -> Result<(), Box<dyn Error>> {
        let file = temp_heap_file()?;
        let mut heap = Heap::open(&file)?;

        heap.write_page(2, &page_contents(3))?;

        assert_eq!(File::open(&file)?.metadata()?.len(), PAGE_SIZE as u64 * 3);

        Ok(())
    }

    fn temp_heap_file() -> io::Result<PathBuf> {
        let mut heap_file = tempfile::tempdir()?.into_path();
        heap_file.push("object");
        Ok(heap_file)
    }

    fn page_contents(n: u8) -> [u8; PAGE_SIZE] {
        let mut a = [0u8; PAGE_SIZE];
        a[0] = n;
        a
    }
}
