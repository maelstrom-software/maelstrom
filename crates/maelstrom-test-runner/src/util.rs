use derive_more::From;
use std::fmt::{self, Debug, Display, Formatter};

#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum NotRunEstimate {
    About(u64),
    Exactly(u64),
    GreaterThan(u64),
    Unknown,
}

impl Display for NotRunEstimate {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::About(n) => Display::fmt(&format!("~{n}"), f),
            Self::Exactly(n) => Display::fmt(&n, f),
            Self::GreaterThan(n) => Display::fmt(&format!(">{n}"), f),
            Self::Unknown => Display::fmt("unknown number of", f),
        }
    }
}

#[derive(Clone, Copy, Debug, derive_more::Display, Eq, From, PartialEq)]
pub struct ListTests(bool);

impl ListTests {
    pub fn as_bool(self) -> bool {
        self.0
    }
}

#[derive(Clone, Copy, Debug, Eq, From, PartialEq)]
pub struct StdoutTty(bool);

impl StdoutTty {
    pub fn as_bool(self) -> bool {
        self.0
    }
}

#[derive(Clone, Copy, Debug, Eq, From, PartialEq)]
pub struct UseColor(bool);

impl UseColor {
    pub fn as_bool(self) -> bool {
        self.0
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn not_run_estimate_display() {
        #[track_caller]
        fn not_run_estimate_test(estimate: NotRunEstimate, expected: &str) {
            assert_eq!(format!("{estimate} tests not run"), expected);
        }

        not_run_estimate_test(NotRunEstimate::About(42), "~42 tests not run");
        not_run_estimate_test(NotRunEstimate::Exactly(42), "42 tests not run");
        not_run_estimate_test(NotRunEstimate::GreaterThan(42), ">42 tests not run");
        not_run_estimate_test(NotRunEstimate::Unknown, "unknown number of tests not run");
    }
}
