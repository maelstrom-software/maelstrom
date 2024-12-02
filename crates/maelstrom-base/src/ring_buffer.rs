//! This implements a simple ring-buffer backed by a vector that has serde support

use serde::{
    ser::{SerializeSeq as _, SerializeStruct as _},
    Deserialize, Serialize, Serializer,
};
use std::{fmt, iter, slice};

#[derive(Clone, Eq, Deserialize)]
#[serde(from = "RingBufferDeserProxy<T>")]
pub struct RingBuffer<T> {
    buf: Vec<T>,
    cursor: usize,
    capacity: usize,
}

impl<T> PartialEq for RingBuffer<T>
where
    T: PartialEq,
{
    fn eq(&self, other: &Self) -> bool {
        self.capacity() == other.capacity() && self.iter().eq(other.iter())
    }
}

impl<T> fmt::Debug for RingBuffer<T>
where
    T: fmt::Debug,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RingBuffer")
            .field("capacity", &self.capacity)
            .field("elements", &Vec::from_iter(self.iter()))
            .finish()
    }
}

impl<T> RingBuffer<T> {
    /// Create a `RingBuffer` that contains capacity many elements
    /// panics if `capacity` is zero
    pub fn new(capacity: usize) -> Self {
        assert!(capacity > 0, "capacity must not be zero");

        let mut buf = vec![];
        buf.reserve_exact(capacity);
        Self {
            buf,
            cursor: 0,
            capacity,
        }
    }

    pub fn capacity(&self) -> usize {
        self.capacity
    }

    pub fn insert(&mut self, element: T) {
        if self.buf.len() < self.capacity() {
            self.buf.push(element);
        } else {
            self.buf[self.cursor] = element;
            self.cursor = (self.cursor + 1) % self.capacity();
        }
    }

    pub fn len(&self) -> usize {
        self.buf.len()
    }

    pub fn is_empty(&self) -> bool {
        self.buf.is_empty()
    }

    pub fn iter(&self) -> RingBufferIter<'_, T> {
        self.buf[self.cursor..]
            .iter()
            .chain(self.buf[0..self.cursor].iter())
    }
}

pub type RingBufferIter<'a, T> = iter::Chain<slice::Iter<'a, T>, slice::Iter<'a, T>>;

#[cfg(test)]
fn insert_test(capacity: usize) {
    let mut r = RingBuffer::new(capacity);
    for i in 0..capacity {
        r.insert(i);

        let expected_len = i + 1;
        assert_eq!(r.len(), expected_len);
    }

    assert_eq!(r.len(), capacity);
    assert_eq!(
        Vec::from_iter(r.iter().copied()),
        Vec::from_iter(0..capacity)
    );

    for i in capacity..=(capacity * 2) {
        r.insert(i);

        assert_eq!(r.len(), capacity);

        let sequence_len = i + 1;
        assert_eq!(
            Vec::from_iter(r.iter().copied()),
            Vec::from_iter((0..sequence_len).skip(sequence_len - capacity))
        );
    }
}

#[test]
fn insert_and_iter() {
    for i in 1..=100 {
        insert_test(i);
    }
}

#[test]
fn equal_with_different_cursor() {
    let mut r1 = RingBuffer::new(3);
    r1.insert(1);
    r1.insert(2);
    r1.insert(3);

    let mut r2 = RingBuffer::new(3);
    r2.insert(1);
    r2.insert(1);
    r2.insert(2);
    r2.insert(3);

    assert_eq!(r1, r2);
}

#[test]
fn not_equal_with_different_elements() {
    let mut r1 = RingBuffer::new(4);
    r1.insert(1);

    let mut r2 = RingBuffer::new(4);
    r2.insert(2);

    assert_ne!(r1, r2);
}

#[test]
fn equal_with_same_elements() {
    let mut r1 = RingBuffer::new(3);
    r1.insert(1);

    let mut r2 = RingBuffer::new(3);
    r2.insert(1);

    assert_eq!(r1, r2);
}

#[test]
fn not_equal_with_different_capacities() {
    let mut r1 = RingBuffer::new(4);
    r1.insert(1);

    let mut r2 = RingBuffer::new(3);
    r2.insert(1);

    assert_ne!(r1, r2);
}

#[test]
fn debug_fmt() {
    let mut r = RingBuffer::new(3);
    for i in 0..4 {
        r.insert(i);
    }
    assert_eq!(
        format!("{r:?}"),
        "RingBuffer { capacity: 3, elements: [1, 2, 3] }"
    )
}

struct RingBufferElements<'a, T>(&'a RingBuffer<T>);

impl<T> Serialize for RingBufferElements<'_, T>
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

impl<T> Serialize for RingBuffer<T>
where
    T: Serialize,
{
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut state = serializer.serialize_struct("RingBuffer", 2)?;
        state.serialize_field("capacity", &self.capacity())?;
        state.serialize_field("elements", &RingBufferElements(self))?;
        state.end()
    }
}

#[derive(Deserialize)]
#[serde(rename = "RingBuffer")]
struct RingBufferDeserProxy<T> {
    capacity: usize,
    elements: Vec<T>,
}

impl<T> From<RingBufferDeserProxy<T>> for RingBuffer<T> {
    fn from(proxy: RingBufferDeserProxy<T>) -> Self {
        let mut buf = proxy.elements;
        let capacity = proxy.capacity;
        buf.reserve_exact(capacity - buf.len());
        Self {
            cursor: 0,
            buf,
            capacity,
        }
    }
}

#[test]
fn serialize_deserialize_half_empty() {
    use serde_test::{assert_tokens, Token};

    let mut r = RingBuffer::new(5);

    r.insert(1);
    r.insert(2);
    r.insert(3);

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

    let mut r = RingBuffer::new(5);

    for i in 1..=5 {
        r.insert(i);
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
