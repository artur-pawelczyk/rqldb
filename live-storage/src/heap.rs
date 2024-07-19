use crate::page::Page;

#[derive(Default)]
struct Heap {
    pages: Vec<Page>,
}

impl Heap {
    fn push(&mut self, tuple: &[u8]) {
        if self.pages.is_empty() {
            self.pages.push(Page::new());
        }

        loop {
            let last = self.pages.len() - 1;
            let page = &mut self.pages[last];
            if page.push(tuple).is_ok() {
                break;
            } else {
                self.pages.push(Page::new());
            }
        }
    }

    fn tuples(&self) -> impl Iterator<Item = &[u8]> {
        self.pages.iter().flat_map(Page::tuples)
    }

    fn allocated_pages(&self) -> usize {
        self.pages.len()
    }
}

#[cfg(test)]
mod tests {
    use crate::test_util::large_tuple;

    use super::*;

    #[test]
    fn test_write_to_heap() {
        let mut heap = Heap::default();
        let expected_tuple = [0, 0, 0, 1];
        heap.push(&expected_tuple);

        let actual_tuple = heap.tuples().next().unwrap();
        assert_eq!(actual_tuple, expected_tuple);
    }

    #[test]
    fn test_heap_larger_than_page() {
        let tuple = large_tuple::<1024>();

        let mut heap = Heap::default();
        for _ in 0..10 {
            heap.push(&tuple);
        }

        assert!(heap.allocated_pages() > 1);
        assert_eq!(heap.tuples().count(), 10);
    }
}
