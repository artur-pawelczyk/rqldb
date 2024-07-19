use core::fmt;
use std::{io::{self, Read, Write}, ops::Range};

const PAGE_SIZE: usize = 8 * 1024;

pub(crate) struct Page {
    contents: [u8; PAGE_SIZE],
}

// TODO: u16 is probably enough
#[derive(Debug)]
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

    const fn self_size() -> usize {
        std::mem::size_of::<u32>() * 2
    }
}

impl From<&LinePointer> for [u8; 8] {
    fn from(value: &LinePointer) -> Self {
        let mut a = [0u8; 8];
        a[0..4].copy_from_slice(&value.0.to_le_bytes());
        a[4..].copy_from_slice(&value.1.to_le_bytes());
        a
    }
}

impl TryFrom<&[u8]> for LinePointer {
    type Error = ();

    fn try_from(value: &[u8]) -> Result<Self, Self::Error> {
        let a = read_u32(value);
        let b = read_u32(&value[4..]);
        Ok(Self(a, b))
    }
}

#[derive(Debug)]
pub enum PageError {
    PageFull,
}

impl fmt::Display for PageError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(self, f)
    }
}

impl std::error::Error for PageError {}

impl Page {
    pub(crate) fn new() -> Self {
        Self { contents: [0; PAGE_SIZE] }
    }

    pub(crate) fn read(mut r: impl Read) -> io::Result<Self> {
        let mut contents = [0u8; PAGE_SIZE];
        r.read_exact(&mut contents)?;

        Ok(Self { contents })
    }

    pub(crate) fn write(&self, mut w: impl Write) -> io::Result<()> {
        w.write_all(&self.contents)
    }

    pub(crate) fn push(&mut self, b: &[u8]) -> Result<(), PageError> {
        let lp = self.reserve_space(b.len())?;
        lp.insert_into(&mut self.contents, b);

        Ok(())
    }

    pub(crate) fn tuples<'a>(&'a self) -> impl Iterator<Item = &'a [u8]> {
        PageIter {
            line_pointers: self.line_pointers(),
            contents: &self.contents,
        }
    }

    fn line_pointers<'a>(&'a self) -> impl Iterator<Item = LinePointer> + 'a {
        LinePointerIter(read_u32(&self.contents), &self.contents[4..])
    }

    fn reserve_space(&mut self, size: usize) -> Result<LinePointer, PageError> {
        let (last_lp, end) = self.line_pointers().enumerate()
            .map(|(i, lp)| (i+1, lp.0))
            .last()
            .unwrap_or((0, self.contents.len() as u32));

        let lp_self_start = last_lp * LinePointer::self_size() + 4;
        let start = end - size as u32;
        if start as usize <= lp_self_start {
            return Err(PageError::PageFull);
        }

        let lp = LinePointer(start, end);

        let lp_bytes = <[u8; 8]>::from(&lp);
        let lp_self_end = lp_self_start + LinePointer::self_size();
        self.contents[lp_self_start..lp_self_end].copy_from_slice(&lp_bytes);

        let new_lp_count = (last_lp + 1) as u32;
        self.contents[0..4].copy_from_slice(&new_lp_count.to_le_bytes());

        Ok(lp)
    }
}

fn read_u32(b: &[u8]) -> u32 {
    u32::from_le_bytes(<[u8; 4]>::try_from(&b[0..4]).unwrap())
}

struct PageIter<'a, I>
where I: Iterator<Item = LinePointer>
{
    line_pointers: I,
    contents: &'a [u8],
}

impl<'a, I> Iterator for PageIter<'a, I>
where I: Iterator<Item = LinePointer>
{
    type Item = &'a [u8];

    fn next(&mut self) -> Option<Self::Item> {
        if let Some(lp) = self.line_pointers.next() {
            dbg!(&lp);
            Some(&self.contents[lp.range()])
        } else {
            None
        }
    }
}

struct LinePointerIter<'a>(u32, &'a [u8]);
impl Iterator for LinePointerIter<'_> {
    type Item = LinePointer;

    fn next(&mut self) -> Option<Self::Item> {
        if self.0 == 0 {
            None
        } else {
            self.0 -= 1;
            let lp = LinePointer::try_from(self.1).ok()?;
            self.1 = &self.1[LinePointer::self_size()..];
            Some(lp)
        }
    }
}

#[cfg(test)]
mod tests {
    use std::error::Error;

    use crate::test_util::large_tuple;

    use super::*;

    #[test]
    fn test_empty_page() {
        let page = Page::new();
        assert_eq!(page.tuples().count(), 0);
    }

    #[test]
    fn test_add_tuple() -> Result<(), Box<dyn Error>> {
        let tuple_1 = [0, 0, 0, 1];
        let tuple_2 = [0, 0, 0, 2];

        let mut page = Page::new();
        page.push(&tuple_1)?;
        page.push(&tuple_2)?;

        assert_eq!(page.tuples().count(), 2);

        let actual_tuple_1 = page.tuples().next().unwrap();
        assert_eq!(actual_tuple_1, tuple_1);

        let actual_tuple_2 = page.tuples().nth(1).unwrap();
        assert_eq!(actual_tuple_2, tuple_2);

        Ok(())
    }

    #[test]
    fn test_recover_tuples_from_file() -> Result<(), Box<dyn Error>> {
        let expected_tuple = [0, 0, 0, 1];

        let mut page = Page::new();
        page.push(&expected_tuple)?;

        let mut buf = Vec::<u8>::new();
        page.write(&mut buf)?;

        let page = Page::read(buf.as_slice())?;
        let actual_tuple = page.tuples().next().unwrap();
        assert_eq!(actual_tuple, expected_tuple);

        Ok(())
    }

    #[test]
    fn test_overflow_page() {
        let tuple = large_tuple::<1024>();

        let mut page = Page::new();
        for _ in 0..10 {
            let _ignore = page.push(&tuple);
        }

        assert!(page.push(&tuple).is_err())
    }
}
