pub use crate::nonempty;

use derive_more::{Deref, Display, Into, IntoIterator};
use serde::{Deserialize, Serialize, Serializer};

#[derive(Debug, Display, derive_more::Error)]
#[display("attempted to create NonEmpty from empty Vec")]
pub struct EmptyError;

#[derive(Clone, Debug, Deref, Deserialize, Eq, Into, IntoIterator, Ord, PartialEq, PartialOrd)]
#[deref(forward)]
#[into_iterator(owned, ref, ref_mut)]
#[serde(try_from = "Vec<T>")]
pub struct NonEmpty<T>(Vec<T>);

#[macro_export]
macro_rules! nonempty {
    ($($elem:expr),+ $(,)?) => {
        $crate::nonempty::NonEmpty::try_from_iter([$($elem),+]).unwrap()
    }
}

impl<T> NonEmpty<T> {
    pub fn try_from_iter(iter: impl IntoIterator<Item = T>) -> Option<Self> {
        Self::try_from(Vec::from_iter(iter)).ok()
    }

    pub fn first(&self) -> &T {
        self.0.first().unwrap()
    }

    pub fn map<U>(self, f: impl FnMut(T) -> U) -> NonEmpty<U> {
        NonEmpty::try_from_iter(self.into_iter().map(f)).unwrap()
    }

    pub fn push(&mut self, elem: T) {
        self.0.push(elem)
    }
}

// Nb. `Serialize` is implemented manually, as serde's `into` container attribute
// requires a `T: Clone` bound which we'd like to avoid.
impl<T> Serialize for NonEmpty<T>
where
    Vec<T>: Serialize,
{
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        self.0.serialize(serializer)
    }
}

impl<T> TryFrom<Vec<T>> for NonEmpty<T> {
    type Error = EmptyError;
    fn try_from(value: Vec<T>) -> Result<Self, Self::Error> {
        if value.is_empty() {
            Err(EmptyError)
        } else {
            Ok(Self(value))
        }
    }
}
