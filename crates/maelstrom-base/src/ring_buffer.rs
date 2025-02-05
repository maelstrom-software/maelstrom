//! This implements a simple ring-buffer backed by a vector that has serde support

use serde::{
    ser::{SerializeSeq as _, SerializeStruct as _},
    Deserialize, Serialize, Serializer,
};
use std::{
    fmt::{self, Debug, Formatter},
    iter::FusedIterator,
    mem::MaybeUninit,
};

#[derive(Deserialize)]
#[serde(from = "RingBufferDeserProxy<T>")]
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
        for elem in &mut self.buf[self.cursor..self.length] {
            unsafe { elem.assume_init_drop() };
        }
        for elem in &mut self.buf[0..self.cursor] {
            unsafe { elem.assume_init_drop() };
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

    pub fn iter(&self) -> RingBufferIter<'_, T, N> {
        RingBufferIter { buf: self, idx: 0 }
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
pub struct RingBufferIter<'a, T, const N: usize> {
    buf: &'a RingBuffer<T, N>,
    idx: usize,
}

impl<'a, T, const N: usize> Iterator for RingBufferIter<'a, T, N> {
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

impl<T, const N: usize> ExactSizeIterator for RingBufferIter<'_, T, N> {
    fn len(&self) -> usize {
        self.buf.length - self.idx
    }
}

impl<T, const N: usize> FusedIterator for RingBufferIter<'_, T, N> {}

struct RingBufferElements<'a, T, const N: usize>(&'a RingBuffer<T, N>);

impl<T, const N: usize> Serialize for RingBufferElements<'_, T, N>
where
    T: Serialize,
{
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut seq = serializer.serialize_seq(Some(self.0.len()))?;
        for e in self.0.iter() {
            seq.serialize_element(e)?;
        }
        seq.end()
    }
}

impl<T, const N: usize> Serialize for RingBuffer<T, N>
where
    T: Serialize,
{
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut state = serializer.serialize_struct("RingBuffer", 2)?;
        state.serialize_field("capacity", &N)?;
        state.serialize_field("elements", &RingBufferElements(self))?;
        state.end()
    }
}

#[derive(Deserialize)]
#[serde(rename = "RingBuffer")]
struct RingBufferDeserProxy<T> {
    elements: Vec<T>,
}

impl<T, const N: usize> From<RingBufferDeserProxy<T>> for RingBuffer<T, N> {
    fn from(proxy: RingBufferDeserProxy<T>) -> Self {
        Self::from_iter(proxy.elements)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rstest::rstest;
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
    fn serialize_deserialize_half_empty() {
        use serde_test::{assert_tokens, Token};

        let mut r = RingBuffer::<i32, 5>::default();

        r.push(1);
        r.push(2);
        r.push(3);

        assert_tokens(
            &r,
            &[
                Token::Struct {
                    name: "RingBuffer",
                    len: 2,
                },
                Token::Str("capacity"),
                Token::U64(5),
                Token::Str("elements"),
                Token::Seq { len: Some(3) },
                Token::I32(1),
                Token::I32(2),
                Token::I32(3),
                Token::SeqEnd,
                Token::StructEnd,
            ],
        )
    }

    #[test]
    fn serialize_deserialize_full() {
        use serde_test::{assert_tokens, Token};

        let mut r = RingBuffer::<i32, 5>::default();

        for i in 1..=5 {
            r.push(i);
        }

        assert_tokens(
            &r,
            &[
                Token::Struct {
                    name: "RingBuffer",
                    len: 2,
                },
                Token::Str("capacity"),
                Token::U64(5),
                Token::Str("elements"),
                Token::Seq { len: Some(5) },
                Token::I32(1),
                Token::I32(2),
                Token::I32(3),
                Token::I32(4),
                Token::I32(5),
                Token::SeqEnd,
                Token::StructEnd,
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

    #[rstest]
    fn drop(#[values(0, 1, 2, 3, 4)] count: i32) {
        struct TestStruct {
            value: i32,
            drop_log: Rc<RefCell<Vec<i32>>>,
        }

        impl Drop for TestStruct {
            fn drop(&mut self) {
                self.drop_log.borrow_mut().push(self.value);
            }
        }

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
}
