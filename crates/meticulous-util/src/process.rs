use std::{
    num::NonZeroU8,
    process::{self, Termination},
};

#[derive(Copy, Clone, Debug, Eq, PartialEq)]
enum ExitCodeInner {
    Success,
    Failure,
    U8(NonZeroU8),
}

/// It's very hard to test with [`std::process::ExitCode`] because it intentionally doesn't
/// implement [`Eq`], [`PartialEq`], etc. This struct fills the same role and can be converted to
/// [`std::process::ExitCode`] if necessary, but since it implements [`Termination`], one shouldn't
/// need to.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub struct ExitCode(ExitCodeInner);

impl ExitCode {
    pub const SUCCESS: Self = ExitCode(ExitCodeInner::Success);
    pub const FAILURE: Self = ExitCode(ExitCodeInner::Failure);
}

impl From<u8> for ExitCode {
    fn from(val: u8) -> Self {
        if val == 0 {
            Self::SUCCESS
        } else {
            ExitCode(ExitCodeInner::U8(unsafe { NonZeroU8::new_unchecked(val) }))
        }
    }
}

impl Termination for ExitCode {
    fn report(self) -> process::ExitCode {
        self.into()
    }
}

// NB: This can't really be tested because we can't do much with process::ExitCode. So don't mess
// it up!
impl From<ExitCode> for process::ExitCode {
    fn from(val: ExitCode) -> Self {
        match val.0 {
            ExitCodeInner::Success => process::ExitCode::SUCCESS,
            ExitCodeInner::Failure => process::ExitCode::FAILURE,
            ExitCodeInner::U8(val) => process::ExitCode::from(val.get()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_zero_equals_success() {
        assert_eq!(ExitCode::from(0), ExitCode::SUCCESS);
    }

    #[test]
    fn test_success_does_not_equal_failure() {
        assert_ne!(ExitCode::SUCCESS, ExitCode::FAILURE);
    }

    #[test]
    fn test_all_combinations() {
        for i in 0u8..255u8 {
            for j in 0u8..255u8 {
                if i == j {
                    assert_eq!(ExitCode::from(i), ExitCode::from(j));
                } else {
                    assert_ne!(ExitCode::from(i), ExitCode::from(j));
                }
            }
        }
    }
}
