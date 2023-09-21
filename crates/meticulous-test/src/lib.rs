#[macro_export]
macro_rules! ceid {
    [$n:expr] => {
        meticulous_base::ClientExecutionId($n)
    };
}

#[macro_export]
macro_rules! cid {
    [$n:expr] => { meticulous_base::ClientId($n) };
}

#[macro_export]
macro_rules! wid {
    [$n:expr] => { meticulous_base::WorkerId($n) };
}

#[macro_export]
macro_rules! eid {
    [$n:expr] => {
        eid!($n, $n)
    };
    [$cid:expr, $ceid:expr] => {
        meticulous_base::ExecutionId(cid![$cid], ceid![$ceid])
    };
}

#[macro_export]
macro_rules! details {
    [1] => {
        meticulous_base::ExecutionDetails {
            program: "test_1".to_string(),
            arguments: vec![],
            layers: vec![],
        }
    };
    [2] => {
        meticulous_base::ExecutionDetails {
            program: "test_2".to_string(),
            arguments: vec!["arg_1".to_string()],
            layers: vec![],
        }
    };
    [3] => {
        meticulous_base::ExecutionDetails {
            program: "test_3".to_string(),
            arguments: vec!["arg_1".to_string(), "arg_2".to_string()],
            layers: vec![],
        }
    };
    [4] => {
        meticulous_base::ExecutionDetails {
            program: "test_4".to_string(),
            arguments: vec!["arg_1".to_string(), "arg_2".to_string(), "arg_3".to_string()],
            layers: vec![],
        }
    };
    [$n:literal] => {
        meticulous_base::ExecutionDetails {
            program: concat!("test_", stringify!($n)).to_string(),
            arguments: vec!["arg_1".to_string()],
            layers: vec![],
        }
    };
    [$n:literal, [$($digest:expr),*]] => {
        {
            let meticulous_base::ExecutionDetails { program, arguments, .. } = details![$n];
            meticulous_base::ExecutionDetails {
                program,
                arguments,
                layers: vec![$(digest!($digest)),*],
            }
        }
    }
}

#[macro_export]
macro_rules! result {
    [1] => {
        meticulous_base::ExecutionResult::Exited(0)
    };
    [2] => {
        meticulous_base::ExecutionResult::Exited(1)
    };
    [3] => {
        meticulous_base::ExecutionResult::Signalled(15)
    };
    [$n:expr] => {
        meticulous_base::ExecutionResult::Exited($n)
    };
}

#[macro_export]
macro_rules! digest {
    [$n:expr] => {
        meticulous_base::Sha256Digest::from($n as u64)
    }
}

#[macro_export]
macro_rules! path_buf {
    ($e:expr) => {
        std::path::Path::new($e).to_path_buf()
    };
}

#[macro_export]
macro_rules! path_buf_vec {
    [$($e:expr),*] => {
        vec![$(path_buf!($e)),*]
    };
}

#[macro_export]
macro_rules! long_path {
    ($prefix:expr, $n:expr) => {
        format!("{}/{:0>64x}", $prefix, $n).into()
    };
    ($prefix:expr, $n:expr, $s:expr) => {
        format!("{}/{:0>64x}.{}", $prefix, $n, $s).into()
    };
}

#[macro_export]
macro_rules! short_path {
    ($prefix:expr, $n:expr) => {
        format!("{}/{:0>16x}", $prefix, $n).into()
    };
    ($prefix:expr, $n:expr, $s:expr) => {
        format!("{}/{:0>16x}.{}", $prefix, $n, $s).into()
    };
}
