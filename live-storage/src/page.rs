use core::fmt;
use std::{io::{self, Read, Write}, ops::Range};

pub const PAGE_SIZE: usize = 8 * 1024;

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

pub(crate) struct PageMut {
    pub(crate) contents: [u8; PAGE_SIZE],
}

impl PageMut {
    pub(crate) fn new() -> Self {
        Self { contents: [0; PAGE_SIZE] }
    }

    pub(crate) fn with_contents(bytes: &[u8]) -> Self {
        debug_assert!(bytes.len() == PAGE_SIZE);
        let mut contents = [0; PAGE_SIZE];
        contents.copy_from_slice(bytes);
        Self { contents }
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
        let lp_count = read_u32(&self.contents);
        let lp_end = lp_count as usize * LinePointer::self_size() + 4;
        debug_assert!(lp_count < 1024);
        LinePointerIter(&self.contents[4..lp_end])
    }

    fn reserve_space(&mut self, size: usize) -> Result<LinePointer, PageError> {
        let (last_lp, tuple_end) = self.line_pointers().enumerate()
            .map(|(i, lp)| (i+1, lp.0))
            .last()
            .unwrap_or((0, self.contents.len() as u32));

        let lp_self_start = last_lp * LinePointer::self_size() + 4;
        let lp_self_end = lp_self_start + LinePointer::self_size();
        let tuple_start = tuple_end - size as u32;
        if tuple_start as usize <= lp_self_end {
            return Err(PageError::PageFull);
        }

        debug_assert!(tuple_start <= 8*1024);
        debug_assert!(tuple_end <= 8*1024);
        let lp = LinePointer(tuple_start, tuple_end);

        let lp_bytes = <[u8; 8]>::from(&lp);
        self.contents[lp_self_start..lp_self_end].copy_from_slice(&lp_bytes);

        let new_lp_count = (last_lp + 1) as u32;
        self.contents[0..4].copy_from_slice(&new_lp_count.to_le_bytes());

        Ok(lp)
    }
}

pub(crate) struct Page<'a> {
    contents: &'a [u8],
}

impl<'a> Page<'a> {
    pub(crate) fn new(contents: &'a [u8]) -> Self {
        debug_assert!(contents.len() == PAGE_SIZE);
        Self { contents }
    }

    pub(crate) fn tuple(&self, n: usize) -> Option<&'a [u8]> {
        self.line_pointer(n).map(|lp| &self.contents[lp.range()])
    }

    fn line_pointer(&self, n: usize) -> Option<LinePointer> {
        let lp_count = read_u32(&self.contents);
        if lp_count as usize > n {
            let lp_start = n * LinePointer::self_size() + 4;
            let lp_end = lp_start + LinePointer::self_size();
            LinePointer::try_from(&self.contents[lp_start..lp_end]).ok()
        } else {
            None
        }
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
            Some(&self.contents[lp.range()])
        } else {
            None
        }
    }
}

struct LinePointerIter<'a>(&'a [u8]);
impl Iterator for LinePointerIter<'_> {
    type Item = LinePointer;

    fn next(&mut self) -> Option<Self::Item> {
        if let Some(lp_bytes) = self.0.get(0..LinePointer::self_size()) {
            let lp = LinePointer::try_from(lp_bytes).unwrap();
            debug_assert!(lp.0 <= 8*1024);
            debug_assert!(lp.1 <= 8*1024);
            self.0 = &self.0[LinePointer::self_size()..];
            Some(lp)
        } else {
            None
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
        let page = PageMut::new();
        assert_eq!(page.tuples().count(), 0);
    }

    #[test]
    fn test_add_tuple() -> Result<(), Box<dyn Error>> {
        let tuple_1 = [0, 0, 0, 1];
        let tuple_2 = [0, 0, 0, 2];

        let mut page = PageMut::new();
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

        let mut page = PageMut::new();
        page.push(&expected_tuple)?;

        let mut buf = Vec::<u8>::new();
        page.write(&mut buf)?;

        let page = PageMut::read(buf.as_slice())?;
        let actual_tuple = page.tuples().next().unwrap();
        assert_eq!(actual_tuple, expected_tuple);

        Ok(())
    }

    #[test]
    fn test_overflow_page() {
        let tuple = large_tuple::<1024>();

        let mut page = PageMut::new();
        for _ in 0..10 {
            let _ignore = page.push(&tuple);
        }

        assert!(page.push(&tuple).is_err())
    }
}
