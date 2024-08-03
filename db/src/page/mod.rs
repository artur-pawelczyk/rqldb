mod tuple_id;

use core::fmt;
use std::{mem::size_of, ops::{Index, IndexMut, Range}};

pub(crate) use tuple_id::TupleId;

pub const PAGE_SIZE: usize = 8 * 1024;

// TODO: u16 is probably enough
type LpIndex = u32;
type LpCountType = u32;
const LP_COUNT: Range<usize> = 0..size_of::<LpCountType>();

#[derive(Clone, Debug)]
struct LinePointer(LpIndex, LpIndex);
impl LinePointer {
    const fn self_size() -> usize {
        size_of::<LpIndex>() * 2
    }
}

impl Index<LinePointer> for [u8] {
    type Output = [u8];

    fn index(&self, index: LinePointer) -> &Self::Output {
        // TODO: Replace with Self::range
        let start = self.len() - (index.1 as usize);
        let end = self.len() - (index.0 as usize);
        &self[start..end]
    }
}

impl IndexMut<LinePointer> for [u8] {
    fn index_mut(&mut self, index: LinePointer) -> &mut Self::Output {
        // TODO: Replace with Self::range
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

struct Header {
    deleted: bool,
}

impl Header {
    const fn size() -> usize {
        1
    }

    fn write(&self, out: &mut [u8]) {
        let deleted: u8 = if self.deleted { 1 } else { 0 };
        out[..Self::size()].copy_from_slice(&[deleted]);
    }
}

impl Default for Header {
    fn default() -> Self {
        Self { deleted: false }
    }
}

pub(crate) struct PageMut<'a> {
    block_id: u32,
    pub(crate) contents: &'a mut [u8],
}

impl<'a> PageMut<'a> {
    pub(crate) fn new(block_id: u32, contents: &'a mut [u8]) -> Self {
        debug_assert!(contents.len() == PAGE_SIZE);
        Self { block_id, contents }
    }

    pub(crate) fn push(&mut self, b: &[u8]) -> Result<TupleId, PageError> {
        let (id, space) = self.reserve_space(b.len() + Header::size())?;
        let header = Header::default();
        header.write(space);
        space[Header::size()..].copy_from_slice(b);
        
        Ok(id)
    }

    fn line_pointers(&'a self) -> impl Iterator<Item = LinePointer> + 'a {
        let lp_count = read_u32(&self.contents);
        let lp_end = lp_count as usize * LinePointer::self_size() + LP_COUNT.len();
        debug_assert!(lp_count < 1024);
        LinePointerIter(&self.contents[4..lp_end])
    }

    fn reserve_space(&mut self, size: usize) -> Result<(TupleId, &mut [u8]), PageError> {
        let (last_lp, tuple_start) = self.line_pointers().enumerate()
            .map(|(i, lp)| (i+1, lp.1))
            .last()
            .unwrap_or((0, 0));

        let lp_self_start = last_lp * LinePointer::self_size() + LP_COUNT.len();
        let lp_self_end = lp_self_start + LinePointer::self_size();
        let tuple_end = tuple_start + size as u32;
        if tuple_end as usize >= PAGE_SIZE - lp_self_end {
            return Err(PageError::PageFull);
        }

        debug_assert!(tuple_start <= 8*1024);
        debug_assert!(tuple_end <= 8*1024);
        let lp = LinePointer(tuple_start, tuple_end);

        let lp_bytes = <[u8; LinePointer::self_size()]>::from(&lp);
        self.contents[lp_self_start..lp_self_end].copy_from_slice(&lp_bytes);

        let new_lp_count = (last_lp + 1) as u32;
        self.contents[LP_COUNT].copy_from_slice(&new_lp_count.to_le_bytes());

        Ok((TupleId { block: self.block_id, offset: last_lp as u32 + 1 }, &mut self.contents[lp]))
    }

    pub(crate) fn delete(&mut self, id: TupleId) {
        let lp = self.line_pointers().nth(id.into()).unwrap(); // TODO: Remove the 'unwrap'
        let mut header = Header::default();
        header.deleted = true;
        let tuple = &mut self.contents[lp];
        header.write(&mut tuple[0..Header::size()]);
    }
}

pub(crate) struct Page<'a> {
    block_id: u32,
    contents: &'a [u8],
    current_tuple: usize,
    tuple_count: usize,
}

impl<'a> Page<'a> {
    pub(crate) fn new(block_id: u32, contents: &'a [u8]) -> Self {
        debug_assert!(contents.len() == PAGE_SIZE);
        let tuple_count = read_u32(contents) as usize;
        let contents = &contents[LP_COUNT.len()..];
        Self { block_id, contents, tuple_count, current_tuple: 0 }
    }

    pub(crate) fn tuple_by_id(&self, tid: TupleId) -> RawTuple<'a> {
        let n = usize::from(tid);

        let mut lp_bytes = [0u8; LinePointer::self_size()];
        let lp_start = LinePointer::self_size() * n;
        let lp_end = lp_start + LinePointer::self_size();
        lp_bytes.copy_from_slice(&self.contents[lp_start..lp_end]);
        let lp = LinePointer::from(lp_bytes);

        RawTuple { id: tid, contents: &self.contents[lp] }
    }

    fn next_internal(&mut self) -> Option<RawTuple<'a>> {
        if self.current_tuple >= self.tuple_count {
            return None;
        }

        let mut lp_bytes = [0u8; LinePointer::self_size()];
        lp_bytes.copy_from_slice(&self.contents[..LinePointer::self_size()]);
        let lp = LinePointer::from(lp_bytes);

        self.contents = &self.contents[LinePointer::self_size()..];
        self.current_tuple += 1;

        let id = TupleId { block: self.block_id, offset: self.current_tuple as u32 };
        Some(RawTuple { id, contents: &self.contents[lp] })
    }
}

impl<'a> Iterator for Page<'a> {
    type Item = RawTuple<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        while let Some(t) = self.next_internal() {
            if t.is_deleted() {
                continue;
            } else {
                return Some(t)
            }
        }

        None
    }
}

pub(crate) struct RawTuple<'a> {
    id: TupleId,
    contents: &'a [u8],
}

impl<'a> RawTuple<'a> {
    pub(crate) fn id(&self) -> TupleId {
        self.id
    }

    pub(crate) fn contents(self) -> &'a [u8] {
        &self.contents[Header::size()..]
    }

    fn is_deleted(&self) -> bool {
        if self.contents[0] == 0 { false } else { true }
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

#[cfg(test)]
mod tests {
    use std::error::Error;

    use super::*;

    #[test]
    fn test_get_tuple_by_id() -> Result<(), Box<dyn Error>> {
        let mut bytes = [0u8; PAGE_SIZE];
        let tuple_1 = [1, 0, 0, 0];
        let tuple_2 = [2, 0, 0, 0];

        let tid_1 = PageMut::new(0, &mut bytes).push(&tuple_1)?;
        let tid_2 = PageMut::new(0, &mut bytes).push(&tuple_2)?;

        let actual_tuple = Page::new(0, &bytes).tuple_by_id(tid_1);
        assert_eq!(actual_tuple.contents(), tuple_1);

        let actual_tuple = Page::new(0, &bytes).tuple_by_id(tid_2);
        assert_eq!(actual_tuple.contents(), tuple_2);

        Ok(())
    }

    #[test]
    fn test_iter_raw_tuple() -> Result<(), Box<dyn Error>> {
        let mut bytes = [0u8; PAGE_SIZE];
        let tuple_1 = [1, 0, 0, 0];
        let tuple_2 = [2, 0, 0, 0];

        let tid_1 = PageMut::new(0, &mut bytes).push(&tuple_1)?;
        let tid_2 = PageMut::new(0, &mut bytes).push(&tuple_2)?;

        let returned_tuples = Page::new(0, &bytes).collect::<Vec<_>>();
        assert_eq!(returned_tuples[0].id, tid_1);
        assert_eq!(returned_tuples[1].id, tid_2);

        Ok(())
    }

    #[test]
    fn test_delete_tuple_from_page() -> Result<(), Box<dyn Error>> {
        let mut buffer = [0u8; PAGE_SIZE];
        let tuple = [1, 0, 0, 0];

        let tid = PageMut::new(0, &mut buffer).push(&tuple)?;
        PageMut::new(0, &mut buffer).push(&tuple)?;
        PageMut::new(0, &mut buffer).delete(tid);

        assert_eq!(Page::new(0, &buffer).count(), 1);

        Ok(())
    }
}
