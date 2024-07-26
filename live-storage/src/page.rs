use core::fmt;
use std::{io::{self, Read, Seek, SeekFrom}, ops::{Index, IndexMut}};

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
        let start = self.len() - (index.1 as usize);
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

impl From<&LinePointer> for [u8; 8] {
    fn from(value: &LinePointer) -> Self {
        let mut a = [0u8; 8];
        a[0..4].copy_from_slice(&value.0.to_le_bytes());
        a[4..].copy_from_slice(&value.1.to_le_bytes());
        a
    }
}

impl From<[u8; 8]> for LinePointer {
    fn from(value: [u8; 8]) -> Self {
        let a = read_u32(&value);
        let b = read_u32(&value[4..]);
        Self(a, b)        
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

pub(crate) struct Page<R>
where R: Read + Seek
{
    contents: TupleReader<R>,
    current_tuple: usize,
    tuple_count: usize,
}

impl<R> Page<R>
where R: Read + Seek
{
    pub(crate) fn new(mut contents: R) -> Self {
        fn size<T: Seek>(b: &mut T) -> usize {
            let size = b.seek(SeekFrom::End(0)).unwrap();
            b.rewind().unwrap();
            size as usize
        }

        debug_assert!(size(&mut contents) == PAGE_SIZE);
        let tuple_count = consume_u32(&mut contents).unwrap() as usize;
        Self { contents: TupleReader(contents), current_tuple: 0, tuple_count }
    }
}

impl<T> Iterator for Page<T>
where T: Read + Seek
{
    type Item = Vec<u8>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.current_tuple >= self.tuple_count {
            return None;
        }

        let lp = self.contents.read_line_pointer(self.current_tuple).ok()??;

        self.current_tuple += 1;

        self.contents.read_tuple(lp).ok()
    }

    fn nth(&mut self, n: usize) -> Option<Self::Item> {
        if self.tuple_count <= n {
            return None;
        }

        let lp = self.contents.read_line_pointer(n).ok()??;

        self.contents.read_tuple(lp).ok()
    }
}

fn read_u32(b: &[u8]) -> u32 {
    u32::from_le_bytes(<[u8; 4]>::try_from(&b[0..4]).unwrap())
}

fn consume_u32(b: &mut impl Read) -> io::Result<u32> {
    let mut buf = [0u8; 4];
    b.read_exact(&mut buf)?;
    Ok(u32::from_le_bytes(buf))
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

struct TupleReader<R: Read + Seek>(R);

impl<R: Read + Seek> TupleReader<R> {
    fn read_tuple(&mut self, lp: LinePointer) -> io::Result<Vec<u8>> {
        let seek_from_end: i64 = lp.1.into();
        self.0.seek(SeekFrom::End(-seek_from_end))?;
        self.read_bytes(lp.1 - lp.0)
    }

    fn read_line_pointer(&mut self, n: usize) -> io::Result<Option<LinePointer>> {
        self.0.rewind()?;
        let lp_count = consume_u32(&mut self.0)?;
        if lp_count as usize >= n {
            let lp_offset = n * LinePointer::self_size() + 4;
            self.0.seek(SeekFrom::Start(lp_offset.try_into().unwrap()))?;
            let mut lp = [0u8; LinePointer::self_size()];
            self.0.read_exact(&mut lp)?;
            Ok(Some(LinePointer::from(lp)))
        } else {
            Ok(None)
        }
    }

    fn read_bytes(&mut self, n: u32) -> io::Result<Vec<u8>> {
        let mut buf = vec![0; n as usize];
        self.0.read_exact(&mut buf)?;
        Ok(buf)
    }
}
