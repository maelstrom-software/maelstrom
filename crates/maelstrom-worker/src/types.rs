use crate::deps::CacheGetStrategy;
use maelstrom_util::cache::{self, fs};

#[derive(
    Clone, Copy, Debug, strum::Display, PartialEq, Eq, PartialOrd, Ord, Hash, strum::EnumIter,
)]
#[strum(serialize_all = "snake_case")]
pub enum CacheKeyKind {
    Blob,
    BottomFsLayer,
    UpperFsLayer,
}

impl cache::KeyKind for CacheKeyKind {
    type Iterator = <Self as strum::IntoEnumIterator>::Iterator;

    fn iter() -> Self::Iterator {
        <Self as strum::IntoEnumIterator>::iter()
    }
}

pub type CacheKey = cache::Key<CacheKeyKind>;
pub type Cache = cache::Cache<fs::std::Fs, CacheKeyKind, CacheGetStrategy>;
pub type TempFileFactory = cache::TempFileFactory<fs::std::Fs>;
