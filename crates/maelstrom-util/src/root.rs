use std::{
    borrow::{Borrow, ToOwned},
    marker::PhantomData,
    ops::Deref,
    path::{Path, PathBuf},
};

pub struct CacheDir;

#[repr(transparent)]
pub struct Root<T: ?Sized> {
    phantom: PhantomData<T>,
    inner: Path,
}

impl<T: ?Sized> Root<T> {
    pub fn new(inner: &Path) -> &Self {
        let ptr = (inner as *const Path) as *const Root<T>;
        unsafe { &*ptr }
    }

    pub fn transmute<U>(&self) -> &Root<U> {
        Root::new(&self.inner)
    }

    pub fn as_path(&self) -> &Path {
        &self.inner
    }

    pub fn join(&self, path: impl AsRef<Path>) -> RootBuf<T> {
        RootBuf::new(self.as_path().join(path.as_ref()))
    }
}

impl<T> Deref for Root<T> {
    type Target = Path;
    fn deref(&self) -> &Self::Target {
        self.as_path()
    }
}

impl<T> AsRef<Root<T>> for Root<T> {
    fn as_ref(&self) -> &Self {
        self
    }
}

impl<T> AsRef<Path> for Root<T> {
    fn as_ref(&self) -> &Path {
        self.as_path()
    }
}

impl<T> ToOwned for Root<T> {
    type Owned = RootBuf<T>;

    fn to_owned(&self) -> Self::Owned {
        Self::Owned::new(self.as_path().to_owned())
    }

    // Provided method
    fn clone_into(&self, target: &mut Self::Owned) {
        self.as_path().clone_into(&mut target.inner)
    }
}

#[repr(transparent)]
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

    pub fn transmute<U>(self) -> RootBuf<U> {
        RootBuf::new(self.into_inner())
    }

    pub fn into_inner(self) -> PathBuf {
        self.inner
    }

    pub fn join(&self, path: impl AsRef<Path>) -> Self {
        Self::new(self.inner.join(path.as_ref()))
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
