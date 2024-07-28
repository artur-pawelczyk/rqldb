use std::{fs::File, io::{self, Read, Seek as _, SeekFrom, Write as _}, path::Path};

use crate::page::{Page, PageMut, PAGE_SIZE};

pub(crate) struct Heap {
    file: File,
}

impl Heap {
    pub(crate) fn open(path: &Path) -> io::Result<Self> {
        Ok(Self {
            file: File::options().read(true).write(true).create(true).open(path)?,
        })
    }

    pub(crate) fn push(&mut self, tuple: &[u8]) -> io::Result<()> {
        if self.file.metadata()?.len() < PAGE_SIZE as u64 {
            self.expand()?;
        }

        let mut buf = [0u8; PAGE_SIZE];
        loop {
            self.file.seek(SeekFrom::End((PAGE_SIZE as i64) * -1))?;
            self.file.read_exact(&mut buf)?;
            let mut page = PageMut::new(&mut buf);
            if page.push(tuple).is_ok() {
                self.file.seek(SeekFrom::End((PAGE_SIZE as i64) * -1))?;
                self.file.write_all(&buf)?;
                break;
            } else {
                self.expand()?;
            }
        }

        self.file.rewind()?;
        Ok(())
    }

    fn expand(&mut self) -> io::Result<()> {
        self.file.seek(SeekFrom::End(0))?;
        self.file.write_all(&[0u8; PAGE_SIZE])?;
        Ok(())
    }

    pub(crate) fn tuples<'a>(&'a self) -> impl Iterator<Item = Vec<u8>> + 'a {
        HeapIter { file: &self.file, tuple_n: 0, buf: [0u8; PAGE_SIZE] }
    }

    #[cfg(test)]
    fn allocated_pages(&self) -> usize {
        self.file.metadata().unwrap().len() as usize / PAGE_SIZE
    }
}

struct HeapIter<'a> {
    file: &'a File,
    buf: [u8; PAGE_SIZE],
    tuple_n: usize,
}

impl<'a> HeapIter<'a> {
    fn new(file: &'a mut File) -> Self {
        file.rewind().unwrap();
        Self { file, buf: [0u8; PAGE_SIZE], tuple_n: 0 }
    }
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
                return Some(tuple.to_vec());
            } else {
                self.tuple_n = 0;
            }
        }
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
        heap.push(&expected_tuple)?;

        let actual_tuple = heap.tuples().next().unwrap();
        assert_eq!(actual_tuple, expected_tuple);

        Ok(())
    }

    #[test]
    fn test_heap_larger_than_page() -> Result<(), Box<dyn Error>> {
        let tuple = large_tuple::<1024>();

        let mut heap = Heap::open(&temp_heap_file()?)?;
        for _ in 0..10 {
            heap.push(&tuple)?;
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
            heap.push(&tuple)?;
        }

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
