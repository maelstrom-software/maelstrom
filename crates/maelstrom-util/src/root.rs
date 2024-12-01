use derive_more::Debug;
use serde::{de::Deserializer, Deserialize};
use std::{
    borrow::{Borrow, ToOwned},
    marker::PhantomData,
    ops::Deref,
    path::{Path, PathBuf},
    str::FromStr,
};

#[derive(Debug)]
#[repr(transparent)]
#[debug("{inner:?}")]
pub struct Root<T: ?Sized> {
    phantom: PhantomData<T>,
    inner: Path,
}

impl<T: ?Sized> Root<T> {
    pub fn new(inner: &Path) -> &Self {
        let ptr = (inner as *const Path) as *const Root<T>;
        unsafe { &*ptr }
    }

    pub fn join<U>(&self, path: impl AsRef<Path>) -> RootBuf<U> {
        RootBuf::new(self.inner.join(path.as_ref()))
    }

    pub fn transmute<U>(&self) -> &Root<U> {
        Root::<U>::new(&self.inner)
    }
}

impl<T> Deref for Root<T> {
    type Target = Path;
    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl<T> AsRef<Root<T>> for Root<T> {
    fn as_ref(&self) -> &Self {
        self
    }
}

impl<T> AsRef<Path> for Root<T> {
    fn as_ref(&self) -> &Path {
        &self.inner
    }
}

impl<T> ToOwned for Root<T> {
    type Owned = RootBuf<T>;

    fn to_owned(&self) -> Self::Owned {
        Self::Owned::new(self.inner.to_owned())
    }

    // Provided method
    fn clone_into(&self, target: &mut Self::Owned) {
        self.inner.clone_into(&mut target.inner)
    }
}

#[derive(Debug)]
#[repr(transparent)]
#[debug("{inner:?}")]
pub struct RootBuf<T: ?Sized> {
    phantom: PhantomData<T>,
    inner: PathBuf,
}

impl<T: ?Sized> RootBuf<T> {
    pub fn new(inner: PathBuf) -> Self {
        Self {
            inner,
            phantom: PhantomData,
        }
    }

    pub fn into_path_buf(self) -> PathBuf {
        self.inner
    }

    pub fn join<U>(&self, path: impl AsRef<Path>) -> RootBuf<U> {
        RootBuf::new(self.inner.join(path.as_ref()))
    }

    pub fn as_root(&self) -> &Root<T> {
        Root::new(self.inner.as_path())
    }
}

impl<T> Deref for RootBuf<T> {
    type Target = Root<T>;
    fn deref(&self) -> &Self::Target {
        self.as_root()
    }
}

impl<T> AsRef<Root<T>> for RootBuf<T> {
    fn as_ref(&self) -> &Root<T> {
        self.as_root()
    }
}

impl<T> AsRef<Path> for RootBuf<T> {
    fn as_ref(&self) -> &Path {
        self.inner.as_ref()
    }
}

impl<T> Borrow<Root<T>> for RootBuf<T> {
    fn borrow(&self) -> &Root<T> {
        self.as_root()
    }
}

impl<T> Clone for RootBuf<T> {
    fn clone(&self) -> Self {
        Self::new(self.inner.clone())
    }
}

impl<'de, T> Deserialize<'de> for RootBuf<T> {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        Ok(Self::new(PathBuf::deserialize(deserializer)?))
    }
}

impl<T> FromStr for RootBuf<T> {
    type Err = <PathBuf as FromStr>::Err;
    fn from_str(value: &str) -> Result<Self, Self::Err> {
        Ok(Self::new(PathBuf::from_str(value)?))
    }
}

impl<T> TryFrom<String> for RootBuf<T> {
    type Error = <RootBuf<T> as FromStr>::Err;
    fn try_from(from: String) -> Result<Self, Self::Error> {
        Self::from_str(from.as_str())
    }
}
