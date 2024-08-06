use std::{fs::File, io::{self, Seek, SeekFrom, Write}, path::Path};

use rqldb::page::{BlockId, PAGE_SIZE};

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
}

#[cfg(test)]
mod tests {
    use std::{error::Error, path::PathBuf};

    use super::*;

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
