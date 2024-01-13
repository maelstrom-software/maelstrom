use std::{
    num::NonZeroU8,
    process::{self, Termination},
    sync::Mutex,
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

pub struct ExitCodeAccumulator(Mutex<ExitCode>);

impl Default for ExitCodeAccumulator {
    fn default() -> Self {
        ExitCodeAccumulator(Mutex::new(ExitCode::SUCCESS))
    }
}

impl ExitCodeAccumulator {
    pub fn add(&self, code: ExitCode) {
        if code != ExitCode::SUCCESS {
            let mut guard = self.0.lock().unwrap();
            if *guard == ExitCode::SUCCESS {
                *guard = code;
            }
        }
    }

    pub fn get(&self) -> ExitCode {
        *self.0.lock().unwrap()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn is_clone<T: Sync>() {}
    fn is_copy<T: Sync>() {}
    fn is_debug<T: Sync>() {}
    fn is_send<T: Send>() {}
    fn is_sync<T: Sync>() {}

    #[test]
    fn test_exit_code_zero_equals_success() {
        assert_eq!(ExitCode::from(0), ExitCode::SUCCESS);
    }

    #[test]
    fn test_exit_code_success_does_not_equal_failure() {
        assert_ne!(ExitCode::SUCCESS, ExitCode::FAILURE);
    }

    #[test]
    fn test_all_exit_code_combinations() {
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

    #[test]
    fn test_exit_code_properties() {
        is_clone::<ExitCode>();
        is_copy::<ExitCode>();
        is_debug::<ExitCode>();
        is_send::<ExitCode>();
        is_sync::<ExitCode>();
    }

    #[test]
    fn test_accumulator_combos() {
        let cases = vec![
            (vec![], ExitCode::SUCCESS),
            (vec![ExitCode::SUCCESS], ExitCode::SUCCESS),
            (vec![ExitCode::FAILURE], ExitCode::FAILURE),
            (
                vec![ExitCode::FAILURE, ExitCode::SUCCESS],
                ExitCode::FAILURE,
            ),
            (
                vec![ExitCode::SUCCESS, ExitCode::FAILURE],
                ExitCode::FAILURE,
            ),
            (
                vec![ExitCode::from(0), ExitCode::from(1), ExitCode::from(2)],
                ExitCode::from(1),
            ),
            (
                vec![ExitCode::from(2), ExitCode::from(1), ExitCode::from(0)],
                ExitCode::from(2),
            ),
        ];
        for (to_add, result) in cases {
            let accum = ExitCodeAccumulator::default();
            for code in to_add {
                accum.add(code);
            }
            assert_eq!(accum.get(), result);
        }
    }

    #[test]
    fn test_exit_code_accumulator_properties() {
        is_clone::<ExitCodeAccumulator>();
        is_copy::<ExitCodeAccumulator>();
        is_debug::<ExitCodeAccumulator>();
        is_send::<ExitCodeAccumulator>();
        is_sync::<ExitCodeAccumulator>();
    }
}
