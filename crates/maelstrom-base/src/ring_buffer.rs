//! This implements a simple ring-buffer backed by a vector that has serde support

use serde::{
    ser::{SerializeSeq as _, SerializeStruct as _},
    Deserialize, Serialize, Serializer,
};
use std::fmt;

#[derive(Clone, Eq, Deserialize)]
#[serde(from = "RingBufferDeserProxy<T>")]
pub struct RingBuffer<T, const N: usize> {
    buf: Vec<T>,
    cursor: usize,
}

impl<T, const N: usize> PartialEq for RingBuffer<T, N>
where
    T: PartialEq,
{
    fn eq(&self, other: &Self) -> bool {
        self.iter().eq(other.iter())
    }
}

impl<T, const N: usize> fmt::Debug for RingBuffer<T, N>
where
    T: fmt::Debug,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RingBuffer")
            .field("capacity", &N)
            .field("elements", &Vec::from_iter(self.iter()))
            .finish()
    }
}

impl<T, const N: usize> Default for RingBuffer<T, N> {
    fn default() -> Self {
        assert!(N > 0, "capacity must not be zero");

        let mut buf = vec![];
        buf.reserve_exact(N);
        Self { buf, cursor: 0 }
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
        if self.buf.len() < N {
            self.buf.push(element);
        } else {
            self.buf[self.cursor] = element;
            self.cursor = (self.cursor + 1) % N;
        }
    }

    pub fn len(&self) -> usize {
        self.buf.len()
    }

    pub fn is_empty(&self) -> bool {
        self.buf.is_empty()
    }

    pub fn iter(&self) -> RingBufferIter<'_, T, N> {
        RingBufferIter {
            ring_buffer: self,
            looped: false,
            offset: self.cursor,
        }
    }
}

#[derive(Clone)]
pub struct RingBufferIter<'a, T, const N: usize> {
    ring_buffer: &'a RingBuffer<T, N>,
    looped: bool,
    offset: usize,
}

impl<'a, T, const N: usize> Iterator for RingBufferIter<'a, T, N> {
    type Item = &'a T;

    fn next(&mut self) -> Option<Self::Item> {
        if !self.looped && self.offset == self.ring_buffer.len() {
            self.looped = true;
            self.offset = 0;
        }
        let upper_bound = if self.looped {
            self.ring_buffer.cursor
        } else {
            self.ring_buffer.len()
        };
        (self.offset < upper_bound).then(|| {
            let result = self.ring_buffer.buf.get(self.offset).unwrap();
            self.offset += 1;
            result
        })
    }
}

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
        let mut buf = proxy.elements;
        buf.reserve_exact(N - buf.len());
        Self { cursor: 0, buf }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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
        let mut r = RingBuffer::<usize, 3>::default();
        for i in 0..4 {
            r.push(i);
        }
        assert_eq!(
            format!("{r:?}"),
            "RingBuffer { capacity: 3, elements: [1, 2, 3] }"
        )
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
}
