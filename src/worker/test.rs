macro_rules! id {
    [$n:expr] => {
        ExecutionId($n)
    };
}

macro_rules! details {
    [1] => {
        ExecutionDetails {
            program: "test_1".to_string(),
            arguments: vec![],
        }
    };
    [2] => {
        ExecutionDetails {
            program: "test_2".to_string(),
            arguments: vec!["arg_1".to_string()],
        }
    };
    [3] => {
        ExecutionDetails {
            program: "test_3".to_string(),
            arguments: vec!["arg_1".to_string(), "arg_2".to_string()],
        }
    };
    [4] => {
        ExecutionDetails {
            program: "test_4".to_string(),
            arguments: vec!["arg_1".to_string(), "arg_2".to_string(), "arg_3".to_string()],
        }
    };
}

macro_rules! result {
    [1] => {
        ExecutionResult::Exited(0)
    };
    [2] => {
        ExecutionResult::Exited(1)
    };
    [3] => {
        ExecutionResult::Signalled
    };
}

pub use details;
pub use id;
pub use result;
