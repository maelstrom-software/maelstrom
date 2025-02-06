//! This implements a simple ring-buffer backed by a vector that has serde support

use serde::{
    de::{Deserializer, SeqAccess, Visitor},
    ser::SerializeSeq as _,
    Deserialize, Serialize, Serializer,
};
use std::{
    fmt::{self, Debug, Formatter},
    iter::FusedIterator,
    marker::PhantomData,
    mem::{self, MaybeUninit},
};

pub struct RingBuffer<T, const N: usize> {
    buf: [MaybeUninit<T>; N],
    length: usize,
    cursor: usize,
}

impl<T, const N: usize> Default for RingBuffer<T, N> {
    fn default() -> Self {
        assert!(N > 0, "capacity must not be zero");
        Self {
            buf: [const { MaybeUninit::uninit() }; N],
            length: 0,
            cursor: 0,
        }
    }
}

impl<T: Clone, const N: usize> Clone for RingBuffer<T, N> {
    fn clone(&self) -> Self {
        Self::from_iter(self.iter().cloned())
    }
}

impl<T, const N: usize> Drop for RingBuffer<T, N> {
    fn drop(&mut self) {
        for i in self.cursor..self.length {
            unsafe { self.buf[i].assume_init_drop() };
        }
        for i in 0..self.cursor {
            unsafe { self.buf[i].assume_init_drop() };
        }
    }
}

impl<T: PartialEq, const N: usize> PartialEq for RingBuffer<T, N> {
    fn eq(&self, other: &Self) -> bool {
        self.iter().eq(other.iter())
    }
}

impl<T: Eq, const N: usize> Eq for RingBuffer<T, N> {}

impl<T: Debug, const N: usize> Debug for RingBuffer<T, N> {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_list().entries(self.iter()).finish()
    }
}

impl<T, const N: usize> FromIterator<T> for RingBuffer<T, N> {
    fn from_iter<IterT: IntoIterator<Item = T>>(iter: IterT) -> Self {
        let mut result = Self::default();
        for item in iter.into_iter() {
            result.push(item)
        }
        result
    }
}

impl<T, const N: usize> RingBuffer<T, N> {
    pub fn push(&mut self, element: T) {
        if self.length < N {
            self.buf[self.length].write(element);
            self.length += 1;
        } else {
            let old = unsafe { self.buf[self.cursor].assume_init_mut() };
            *old = element;
            self.cursor = (self.cursor + 1) % N;
        }
    }

    pub fn len(&self) -> usize {
        self.length
    }

    pub fn is_empty(&self) -> bool {
        self.length == 0
    }

    pub fn iter(&self) -> Iter<'_, T, N> {
        Iter { buf: self, idx: 0 }
    }

    pub fn get(&self, i: usize) -> Option<&T> {
        if i >= self.length {
            None
        } else {
            let i = if self.cursor < N - i {
                self.cursor + i
            } else {
                self.cursor - (N - i)
            };
            Some(unsafe { self.buf[i].assume_init_ref() })
        }
    }
}

#[derive(Clone)]
pub struct Iter<'a, T, const N: usize> {
    buf: &'a RingBuffer<T, N>,
    idx: usize,
}

impl<'a, T, const N: usize> Iterator for Iter<'a, T, N> {
    type Item = &'a T;

    fn next(&mut self) -> Option<Self::Item> {
        let result = self.buf.get(self.idx);
        if result.is_some() {
            self.idx += 1;
        }
        result
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let size = self.len();
        (size, Some(size))
    }
}

impl<T, const N: usize> ExactSizeIterator for Iter<'_, T, N> {
    fn len(&self) -> usize {
        self.buf.length - self.idx
    }
}

impl<T, const N: usize> FusedIterator for Iter<'_, T, N> {}

impl<'a, T, const N: usize> IntoIterator for &'a RingBuffer<T, N> {
    type Item = &'a T;
    type IntoIter = Iter<'a, T, N>;
    fn into_iter(self) -> Self::IntoIter {
        self.iter()
    }
}

impl<T, const N: usize> IntoIterator for RingBuffer<T, N> {
    type Item = T;
    type IntoIter = IntoIter<T, N>;
    fn into_iter(mut self) -> Self::IntoIter {
        let buf = mem::replace(&mut self.buf, [const { MaybeUninit::uninit() }; N]);
        let length = mem::take(&mut self.length);
        let cursor = mem::take(&mut self.cursor);
        if length < N {
            IntoIter {
                buf,
                first_beg: 0,
                first_end: length,
                rest_beg: 0,
                rest_end: 0,
            }
        } else {
            IntoIter {
                buf,
                first_beg: cursor,
                first_end: N,
                rest_beg: 0,
                rest_end: cursor,
            }
        }
    }
}

pub struct IntoIter<T, const N: usize> {
    buf: [MaybeUninit<T>; N],
    first_beg: usize,
    first_end: usize,
    rest_beg: usize,
    rest_end: usize,
}

impl<T, const N: usize> Drop for IntoIter<T, N> {
    fn drop(&mut self) {
        for i in self.first_beg..self.first_end {
            unsafe { self.buf[i].assume_init_drop() };
        }
        for i in self.rest_beg..self.rest_end {
            unsafe { self.buf[i].assume_init_drop() };
        }
    }
}

impl<T, const N: usize> Iterator for IntoIter<T, N> {
    type Item = T;

    fn next(&mut self) -> Option<Self::Item> {
        if self.first_beg < self.first_end {
            let loc = mem::replace(&mut self.buf[self.first_beg], MaybeUninit::uninit());
            self.first_beg += 1;
            Some(unsafe { loc.assume_init() })
        } else if self.rest_beg < self.rest_end {
            let loc = mem::replace(&mut self.buf[self.rest_beg], MaybeUninit::uninit());
            self.rest_beg += 1;
            Some(unsafe { loc.assume_init() })
        } else {
            None
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let size = self.len();
        (size, Some(size))
    }
}

impl<T, const N: usize> ExactSizeIterator for IntoIter<T, N> {
    fn len(&self) -> usize {
        (self.first_end - self.first_beg) + (self.rest_end - self.rest_beg)
    }
}

impl<T, const N: usize> FusedIterator for IntoIter<T, N> {}

impl<T, const N: usize> Serialize for RingBuffer<T, N>
where
    T: Serialize,
{
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut seq = serializer.serialize_seq(Some(self.len()))?;
        for element in self {
            seq.serialize_element(element)?;
        }
        seq.end()
    }
}

struct DeserializeVisitor<T, const N: usize>(PhantomData<fn() -> RingBuffer<T, N>>);

impl<T, const N: usize> DeserializeVisitor<T, N> {
    fn new() -> Self {
        Self(PhantomData)
    }
}

impl<'de, T: Deserialize<'de>, const N: usize> Visitor<'de> for DeserializeVisitor<T, N> {
    type Value = RingBuffer<T, N>;

    fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        formatter.write_str("a RingBuffer sequence")
    }

    fn visit_seq<A: SeqAccess<'de>>(self, mut access: A) -> Result<Self::Value, A::Error> {
        let mut result = RingBuffer::default();
        while let Some(elem) = access.next_element()? {
            result.push(elem);
        }
        Ok(result)
    }
}

// This is the trait that informs Serde how to deserialize MyMap.
impl<'de, T, const N: usize> Deserialize<'de> for RingBuffer<T, N>
where
    T: Deserialize<'de>,
{
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_seq(DeserializeVisitor::new())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rstest::rstest;
    use serde_test::{assert_tokens, Token};
    use std::{cell::RefCell, mem, rc::Rc};

    #[track_caller]
    fn insert_test<const N: usize>() {
        let mut r = RingBuffer::<usize, N>::default();

        for i in 0..N {
            assert_eq!(r.len(), i);
            r.push(i);
        }
        assert_eq!(r.len(), N);
        assert_eq!(Vec::from_iter(r.iter().copied()), Vec::from_iter(0..N),);

        for i in N..=(N * 2) {
            r.push(i);
            assert_eq!(r.len(), N);
            assert_eq!(
                Vec::from_iter(r.iter().copied()),
                Vec::from_iter(i - N + 1..=i),
            );
        }
    }

    macro_rules! insert_test {
        ($name:ident, $n:expr) => {
            #[test]
            fn $name() {
                insert_test::<$n>();
            }
        };
    }

    insert_test!(insert_test_1, 1);
    insert_test!(insert_test_2, 2);
    insert_test!(insert_test_3, 3);
    insert_test!(insert_test_10, 10);
    insert_test!(insert_test_100, 100);

    #[test]
    fn equal_with_different_cursor() {
        let mut r1 = RingBuffer::<usize, 3>::default();
        r1.push(1);
        r1.push(2);
        r1.push(3);

        let mut r2 = RingBuffer::<usize, 3>::default();
        r2.push(1);
        r2.push(1);
        r2.push(2);
        r2.push(3);

        assert_eq!(r1, r2);
    }

    #[test]
    fn not_equal_with_different_elements() {
        let mut r1 = RingBuffer::<usize, 4>::default();
        r1.push(1);

        let mut r2 = RingBuffer::<usize, 4>::default();
        r2.push(2);

        assert_ne!(r1, r2);
    }

    #[test]
    fn equal_with_same_elements() {
        let mut r1 = RingBuffer::<usize, 3>::default();
        r1.push(1);

        let mut r2 = RingBuffer::<usize, 3>::default();
        r2.push(1);

        assert_eq!(r1, r2);
    }

    #[test]
    fn debug_fmt() {
        assert_eq!(
            format!("{:?}", RingBuffer::<_, 2>::from_iter::<[usize; 0]>([])),
            "[]",
        );
        assert_eq!(format!("{:?}", RingBuffer::<_, 2>::from_iter([1])), "[1]");
        assert_eq!(
            format!("{:?}", RingBuffer::<_, 2>::from_iter([1, 2])),
            "[1, 2]",
        );
        assert_eq!(
            format!("{:?}", RingBuffer::<_, 2>::from_iter([1, 2, 3])),
            "[2, 3]",
        );
    }

    #[test]
    fn serialize_deserialize_empty() {
        let r = RingBuffer::<i32, 5>::from_iter([]);
        assert_tokens(&r, &[Token::Seq { len: Some(0) }, Token::SeqEnd])
    }

    #[test]
    fn serialize_deserialize_half_empty() {
        let r = RingBuffer::<i32, 5>::from_iter(1..=3);
        assert_tokens(
            &r,
            &[
                Token::Seq { len: Some(3) },
                Token::I32(1),
                Token::I32(2),
                Token::I32(3),
                Token::SeqEnd,
            ],
        )
    }

    #[test]
    fn serialize_deserialize_full() {
        let r = RingBuffer::<i32, 5>::from_iter(1..=5);
        assert_tokens(
            &r,
            &[
                Token::Seq { len: Some(5) },
                Token::I32(1),
                Token::I32(2),
                Token::I32(3),
                Token::I32(4),
                Token::I32(5),
                Token::SeqEnd,
            ],
        )
    }

    #[test]
    fn from_iterator() {
        let mut r = RingBuffer::<_, 3>::default();
        assert_eq!(r, RingBuffer::<_, 3>::from_iter([]));
        r.push(1);
        assert_eq!(r, RingBuffer::<_, 3>::from_iter([1]));
        r.push(2);
        assert_eq!(r, RingBuffer::<_, 3>::from_iter([1, 2]));
        r.push(3);
        assert_eq!(r, RingBuffer::<_, 3>::from_iter([1, 2, 3]));
        r.push(4);
        assert_eq!(r, RingBuffer::<_, 3>::from_iter([2, 3, 4]));
    }

    struct TestStruct {
        value: i32,
        drop_log: Rc<RefCell<Vec<i32>>>,
    }

    impl Drop for TestStruct {
        fn drop(&mut self) {
            self.drop_log.borrow_mut().push(self.value);
        }
    }

    #[rstest]
    fn drop(#[values(0, 1, 2, 3, 4)] count: i32) {
        let log = Rc::new(RefCell::new(Vec::<_>::new()));
        let mut r = RingBuffer::<_, 2>::default();
        for i in 1..=count {
            r.push(TestStruct {
                value: i,
                drop_log: log.clone(),
            });
        }
        log.borrow_mut().clear();
        mem::drop(r);

        let expected = match count {
            0 => vec![],
            1 => vec![1],
            n => vec![n - 1, n],
        };
        assert_eq!(*log.borrow(), expected);
    }

    #[rstest]
    fn iter(#[values(0, 1, 2, 3, 4, 5)] count: usize) {
        let r = RingBuffer::<_, 2>::from_iter(0..count);
        let mut i = r.iter();
        if count > 1 {
            assert_eq!(i.len(), 2);
            assert_eq!(i.size_hint(), (2, Some(2)));
            assert_eq!(i.next(), Some(count - 2).as_ref());
        }
        if count > 0 {
            assert_eq!(i.len(), 1);
            assert_eq!(i.size_hint(), (1, Some(1)));
            assert_eq!(i.next(), Some(count - 1).as_ref());
        }
        for _ in 0..1_000_000 {
            assert_eq!(i.len(), 0);
            assert_eq!(i.size_hint(), (0, Some(0)));
            assert_eq!(i.next(), None);
        }
    }

    #[rstest]
    fn into_iter(#[values(0, 1, 2, 3, 4, 5)] count: usize) {
        let r = RingBuffer::<_, 2>::from_iter(0..count);
        let expected = match count {
            0 => vec![],
            1 => vec![0],
            n => vec![n - 2, n - 1],
        };
        assert_eq!(r.into_iter().collect::<Vec<_>>(), expected);
    }

    #[rstest]
    #[case(0, 0)]
    #[case(1, 0)]
    #[case(1, 1)]
    #[case(2, 0)]
    #[case(2, 1)]
    #[case(2, 2)]
    #[case(3, 0)]
    #[case(3, 1)]
    #[case(3, 2)]
    #[case(4, 0)]
    #[case(4, 1)]
    #[case(4, 2)]
    fn into_iter_drop(#[case] to_insert: i32, #[case] to_consume: i32) {
        let log = Rc::new(RefCell::new(Vec::<_>::new()));
        let mut r = RingBuffer::<_, 2>::default();
        for i in 0..to_insert {
            r.push(TestStruct {
                value: i,
                drop_log: log.clone(),
            });
        }
        let mut iter = r.into_iter();
        for _ in 0..to_consume {
            iter.next().unwrap();
        }
        log.borrow_mut().clear();
        mem::drop(iter);

        let expected = match to_insert.min(2) - to_consume {
            0 => vec![],
            1 => vec![to_insert - 1],
            _ => vec![to_insert - 2, to_insert - 1],
        };
        assert_eq!(*log.borrow(), expected);
    }
}
