use std::{io::{self, Read, Write}, ops::Range};

use rqldb_persist::ByteReader;

const PAGE_SIZE: usize = 8 * 1024;

pub(crate) struct Page {
    line_pointers: Vec<LinePointer>,
    contents: [u8; PAGE_SIZE],
}

// TODO: u16 is probably enough
struct LinePointer(u32, u32);
impl LinePointer {
    fn insert_into(&self, target: &mut [u8], source: &[u8]) {
        let start = self.0 as usize;
        let end = self.1 as usize;
        for i in start..end {
            target[i] = source[i - start];
        }
    }

    fn range(&self) -> Range<usize> {
        let start = self.0 as usize;
        let end = self.1 as usize;
        start..end
    }
}

impl Page {
    pub(crate) fn new() -> Self {
        Self { line_pointers: Vec::new(), contents: [0; PAGE_SIZE] }
    }

    pub(crate) fn read(r: impl Read) -> io::Result<Self> {
        let mut r = ByteReader::new(r);
        let mut line_pointers = Vec::new();
        for _ in 0..r.read_u32()? {
            line_pointers.push(LinePointer(r.read_u32()?, r.read_u32()?));
        }

        let mut contents = [0u8; PAGE_SIZE];
        r.read_exact(&mut contents)?;

        Ok(Self { line_pointers, contents })
    }

    pub(crate) fn write(&self, mut w: impl Write) -> io::Result<()> {
        let lp_count = self.line_pointers.len() as u32;
        w.write_all(&lp_count.to_le_bytes())?;
        for lp in &self.line_pointers {
            w.write_all(&lp.0.to_le_bytes())?;
            w.write_all(&lp.1.to_le_bytes())?;
        }

        w.write_all(&self.contents)?;

        Ok(())
    }

    pub(crate) fn push(&mut self, b: &[u8]) {
        let last = self.last_tuple().unwrap_or(0);
        let lp = LinePointer(last as u32, last + b.len() as u32);
        lp.insert_into(&mut self.contents, b);
        self.line_pointers.push(lp);
    }

    pub(crate) fn tuples<'a>(&'a self) -> impl Iterator<Item = &'a [u8]> {
        PageIter {
            line_pointers: &self.line_pointers,
            contents: &self.contents,
        }
    }

    // TODO: Grow tuples from the bottom, and line pointers from the top
    fn last_tuple(&self) -> Option<u32> {
        self.line_pointers.last().map(|lp| lp.1)
    }
}

struct PageIter<'a> {
    line_pointers: &'a [LinePointer],
    contents: &'a [u8],
}

impl<'a> Iterator for PageIter<'a> {
    type Item = &'a [u8];

    fn next(&mut self) -> Option<Self::Item> {
        if let Some(lp) = self.line_pointers.get(0) {
            self.line_pointers = &self.line_pointers[1..];
            Some(&self.contents[lp.range()])
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
    fn test_empty_page() {
        let page = Page::new();
        assert_eq!(page.tuples().count(), 0);
    }

    #[test]
    fn test_add_tuple() {
        let tuple_1 = [0, 0, 0, 1];
        let tuple_2 = [0, 0, 0, 2];

        let mut page = Page::new();
        page.push(&tuple_1);
        page.push(&tuple_2);

        assert_eq!(page.tuples().count(), 2);

        let actual_tuple_1 = page.tuples().next().unwrap();
        assert_eq!(actual_tuple_1, tuple_1);

        let actual_tuple_2 = page.tuples().nth(1).unwrap();
        assert_eq!(actual_tuple_2, tuple_2);
    }

    #[test]
    fn test_recover_tuples_from_file() -> Result<(), Box<dyn Error>> {
        let expected_tuple = [0, 0, 0, 1];

        let mut page = Page::new();
        page.push(&expected_tuple);

        let mut buf = Vec::<u8>::new();
        page.write(&mut buf)?;

        let page = Page::read(buf.as_slice())?;
        let actual_tuple = page.tuples().next().unwrap();
        assert_eq!(actual_tuple, expected_tuple);

        Ok(())
    }
}
