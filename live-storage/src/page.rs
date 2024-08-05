use std::{mem::size_of, ops::{Index, IndexMut, Range}};

pub const PAGE_SIZE: usize = 8 * 1024;

// TODO: u16 is probably enough
type LpIndex = u32;
type LpCountType = u32;
const LP_COUNT: Range<usize> = 0..size_of::<LpCountType>();
const HEADER_SIZE: usize = 1;

#[derive(Debug)]
struct LinePointer(LpIndex, LpIndex);
impl LinePointer {
    const fn self_size() -> usize {
        size_of::<LpIndex>() * 2
    }
}

impl Index<LinePointer> for [u8] {
    type Output = [u8];

    fn index(&self, index: LinePointer) -> &Self::Output {
        let start = self.len() - (index.1 as usize) + HEADER_SIZE;
        let end = self.len() - (index.0 as usize);
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

impl From<&LinePointer> for [u8; LinePointer::self_size()] {
    fn from(value: &LinePointer) -> Self {
        let mut a = [0u8; LinePointer::self_size()];
        a[0..size_of::<LpIndex>()].copy_from_slice(&value.0.to_le_bytes());
        a[size_of::<LpIndex>()..].copy_from_slice(&value.1.to_le_bytes());
        a
    }
}

impl From<[u8; LinePointer::self_size()]> for LinePointer {
    fn from(value: [u8; LinePointer::self_size()]) -> Self {
        let a = read_u32(&value);
        let b = read_u32(&value[size_of::<LpIndex>()..]);
        Self(a, b)        
    }
}

impl TryFrom<&[u8]> for LinePointer {
    type Error = ();

    fn try_from(value: &[u8]) -> Result<Self, Self::Error> {
        let a = read_u32(value);
        let b = read_u32(&value.index(size_of::<LpIndex>()..));
        Ok(Self(a, b))
    }
}

pub(crate) struct Page<'a> {
    contents: &'a [u8],
    tuple_count: usize,
}

impl<'a> Page<'a> {
    pub(crate) fn new(contents: &'a [u8]) -> Self {
        debug_assert!(contents.len() == PAGE_SIZE);
        let tuple_count = read_u32(contents) as usize;
        let contents = &contents[LP_COUNT.len()..];
        Self { contents, tuple_count }
    }
}

impl<'a> Iterator for Page<'a> {
    type Item = &'a [u8];

    fn next(&mut self) -> Option<Self::Item> {
        if self.tuple_count <= 0 {
            return None;
        }

        let mut lp_bytes = [0u8; LinePointer::self_size()];
        lp_bytes.copy_from_slice(&self.contents[..LinePointer::self_size()]);
        let lp = LinePointer::from(lp_bytes);

        self.contents = &self.contents[LinePointer::self_size()..];
        self.tuple_count -= 1;

        Some(&self.contents[lp])
    }

    fn nth(&mut self, n: usize) -> Option<Self::Item> {
        if self.tuple_count <= n {
            return None;
        }

        let mut lp_bytes = [0u8; LinePointer::self_size()];
        let lp_start = LinePointer::self_size() * n;
        let lp_end = lp_start + LinePointer::self_size();
        lp_bytes.copy_from_slice(&self.contents[lp_start..lp_end]);
        let lp = LinePointer::from(lp_bytes);

        Some(&self.contents[lp])
    }
}

fn read_u32(b: &[u8]) -> u32 {
    u32::from_le_bytes(<[u8; 4]>::try_from(&b[0..4]).unwrap())
}

