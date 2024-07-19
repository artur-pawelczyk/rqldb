pub(crate) fn large_tuple<const N: usize>() -> [u8; N] {
    let mut v = [0; N];
    v[0] = 123;
    v
}
