use core::fmt;
use std::ops::{Index, IndexMut};

pub const PAGE_SIZE: usize = 8 * 1024;

// TODO: u16 is probably enough
#[derive(Debug)]
struct LinePointer(u32, u32);
impl LinePointer {
    const fn self_size() -> usize {
        std::mem::size_of::<u32>() * 2
    }
}

impl Index<LinePointer> for [u8] {
    type Output = [u8];

    fn index(&self, index: LinePointer) -> &Self::Output {
        let start = PAGE_SIZE - (index.1 as usize);
        let end = PAGE_SIZE - (index.0 as usize);
        &self[start..end]
    }
}

impl IndexMut<LinePointer> for [u8] {
    fn index_mut(&mut self, index: LinePointer) -> &mut Self::Output {
        let start = PAGE_SIZE - (index.1 as usize);
        let end = PAGE_SIZE - (index.0 as usize);
        &mut self[start..end]
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
        let b = read_u32(&value.index(4..));
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

pub(crate) struct PageMut<'a> {
    pub(crate) contents: &'a mut [u8],
}

impl<'a> PageMut<'a> {
    pub(crate) fn new(contents: &'a mut [u8]) -> Self {
        debug_assert!(contents.len() == PAGE_SIZE);
        Self { contents }
    }

    pub(crate) fn push(&mut self, b: &[u8]) -> Result<(), PageError> {
        let lp = self.reserve_space(b.len())?;
        self.contents[lp].copy_from_slice(b);

        Ok(())
    }

    fn line_pointers(&'a self) -> impl Iterator<Item = LinePointer> + 'a {
        let lp_count = read_u32(&self.contents);
        let lp_end = lp_count as usize * LinePointer::self_size() + 4;
        debug_assert!(lp_count < 1024);
        LinePointerIter(&self.contents[4..lp_end])
    }

    fn reserve_space(&mut self, size: usize) -> Result<LinePointer, PageError> {
        let (last_lp, tuple_start) = self.line_pointers().enumerate()
            .map(|(i, lp)| (i+1, lp.1))
            .last()
            .unwrap_or((0, 0));

        let lp_self_start = last_lp * LinePointer::self_size() + 4;
        let lp_self_end = lp_self_start + LinePointer::self_size();
        let tuple_end = tuple_start + size as u32;
        if tuple_end as usize >= PAGE_SIZE - lp_self_end {
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
        self.line_pointer(n).map(|lp| &self.contents[lp])
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

