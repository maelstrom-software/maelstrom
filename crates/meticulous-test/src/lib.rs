#[macro_export]
macro_rules! cjid {
    [$n:expr] => {
        meticulous_base::ClientJobId::from($n)
    };
}

#[macro_export]
macro_rules! cid {
    [$n:expr] => { meticulous_base::ClientId::from($n) };
}

#[macro_export]
macro_rules! wid {
    [$n:expr] => { meticulous_base::WorkerId::from($n) };
}

#[macro_export]
macro_rules! jid {
    [$n:expr] => {
        jid!($n, $n)
    };
    [$cid:expr, $cjid:expr] => {
        meticulous_base::JobId{cid: cid![$cid], cjid: cjid![$cjid]}
    };
}

#[macro_export]
macro_rules! details {
    [1] => {
        meticulous_base::JobDetails {
            program: "test_1".to_string(),
            arguments: vec![],
            layers: vec![],
        }
    };
    [2] => {
        meticulous_base::JobDetails {
            program: "test_2".to_string(),
            arguments: vec!["arg_1".to_string()],
            layers: vec![],
        }
    };
    [3] => {
        meticulous_base::JobDetails {
            program: "test_3".to_string(),
            arguments: vec!["arg_1".to_string(), "arg_2".to_string()],
            layers: vec![],
        }
    };
    [4] => {
        meticulous_base::JobDetails {
            program: "test_4".to_string(),
            arguments: vec!["arg_1".to_string(), "arg_2".to_string(), "arg_3".to_string()],
            layers: vec![],
        }
    };
    [$n:literal] => {
        meticulous_base::JobDetails {
            program: concat!("test_", stringify!($n)).to_string(),
            arguments: vec!["arg_1".to_string()],
            layers: vec![],
        }
    };
    [$n:literal, [$($digest:expr),*]] => {
        {
            let meticulous_base::JobDetails { program, arguments, .. } = details![$n];
            meticulous_base::JobDetails {
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
        meticulous_base::JobResult::Ran{status: meticulous_base::JobStatus::Exited(0)}
    };
    [2] => {
        meticulous_base::JobResult::Ran{status: meticulous_base::JobStatus::Exited(1)}
    };
    [3] => {
        meticulous_base::JobResult::Ran{status: meticulous_base::JobStatus::Signalled(15)}
    };
    [$n:expr] => {
        meticulous_base::JobResult::Ran{status: meticulous_base::JobStatus::Exited($n)}
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
