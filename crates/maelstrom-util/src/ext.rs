//! Useful extension traits.

/// An extension trait that is useful for working with [`std::collections::HashMap`]s. A lot of
/// times we just want to assert whether or not something previously existed in the
/// [`std::collections::HashMap`]. This provides a convenient way to do that.
pub trait OptionExt<T> {
    fn assert_is_none(self);
    fn assert_is_some(self);
    fn expect_is_none(self, f: impl Fn(T) -> String);
    fn expect_is_some(self, f: impl Fn() -> String) -> T;
}

impl<T> OptionExt<T> for Option<T> {
    #[track_caller]
    fn assert_is_none(self) {
        assert!(self.is_none());
    }
    #[track_caller]
    fn assert_is_some(self) {
        assert!(self.is_some());
    }
    #[track_caller]
    fn expect_is_none(self, f: impl Fn(T) -> String) {
        if let Some(val) = self {
            panic!("{}", f(val));
        }
    }
    #[track_caller]
    fn expect_is_some(self, f: impl Fn() -> String) -> T {
        match self {
            None => {
                panic!("{}", f());
            }
            Some(val) => val,
        }
    }
}

/// An extension trait that is useful for working with [`std::collections::HashSet`]s. A lot of
/// times we just want to assert whether or not something previously existed in the
/// [`std::collections::HashSet`]. This provides a convenient way to do that.
pub trait BoolExt {
    fn assert_is_true(self);
    fn assert_is_false(self);
}

impl BoolExt for bool {
    #[track_caller]
    fn assert_is_true(self) {
        assert!(self);
    }

    #[track_caller]
    fn assert_is_false(self) {
        assert!(!self);
    }
}
