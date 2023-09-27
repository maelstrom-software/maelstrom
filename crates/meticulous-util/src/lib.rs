pub mod error;
pub mod heap;
pub mod net;

/// An extension trait that is useful for working with [std::collections::HashMap]s. A lot of times
/// we just want to assert whether or not something previously existed in the
/// [std::collections::HashMap]. This provides a convenient way to do that.
pub trait OptionExt {
    fn assert_is_none(self);
    fn assert_is_some(self);
}

impl<T> OptionExt for Option<T> {
    fn assert_is_none(self) {
        assert!(self.is_none());
    }
    fn assert_is_some(self) {
        assert!(self.is_some());
    }
}

/// An extension trait that is useful for working with [std::collections::HashSet]s. A lot of times
/// we just want to assert whether or not something previously existed in the
/// [std::collections::HashSet]. This provides a convenient way to do that.
pub trait BoolExt {
    fn assert_is_true(self);
    fn assert_is_false(self);
}

impl BoolExt for bool {
    fn assert_is_true(self) {
        assert!(self);
    }
    fn assert_is_false(self) {
        assert!(!self);
    }
}
