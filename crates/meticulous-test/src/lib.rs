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
macro_rules! spec {
    [1] => {
        meticulous_base::JobSpec {
            program: "test_1".to_string(),
            arguments: vec![],
            environment: vec![],
            layers: meticulous_base::nonempty![digest!(1)],
            devices: meticulous_base::EnumSet::EMPTY,
            mounts: vec![],
            enable_loopback: false,
            working_directory: std::path::PathBuf::from("/"),
        }
    };
    [2] => {
        meticulous_base::JobSpec {
            program: "test_2".to_string(),
            arguments: vec!["arg_1".to_string()],
            environment: vec![],
            layers: meticulous_base::nonempty![digest!(2)],
            devices: meticulous_base::EnumSet::EMPTY,
            mounts: vec![],
            enable_loopback: false,
            working_directory: std::path::PathBuf::from("/"),
        }
    };
    [3] => {
        meticulous_base::JobSpec {
            program: "test_3".to_string(),
            arguments: vec!["arg_1".to_string(), "arg_2".to_string()],
            environment: vec![],
            layers: meticulous_base::nonempty![digest!(3)],
            devices: meticulous_base::EnumSet::EMPTY,
            mounts: vec![],
            enable_loopback: false,
            working_directory: std::path::PathBuf::from("/"),
        }
    };
    [4] => {
        meticulous_base::JobSpec {
            program: "test_4".to_string(),
            arguments: vec!["arg_1".to_string(), "arg_2".to_string(), "arg_3".to_string()],
            environment: vec![],
            layers: meticulous_base::nonempty![digest!(4)],
            devices: meticulous_base::EnumSet::EMPTY,
            mounts: vec![],
            enable_loopback: false,
            working_directory: std::path::PathBuf::from("/"),
        }
    };
    [$n:literal] => {
        meticulous_base::JobSpec {
            program: concat!("test_", stringify!($n)).to_string(),
            arguments: vec!["arg_1".to_string()],
            environment: vec![],
            layers: meticulous_base::nonempty![digest!($n)],
            devices: meticulous_base::EnumSet::EMPTY,
            mounts: vec![],
            enable_loopback: false,
            working_directory: std::path::PathBuf::from("/"),
        }
    };
    [$n:literal, [$($digest:expr),*]] => {
        {
            let meticulous_base::JobSpec { program, arguments, environment, .. } = spec![$n];
            meticulous_base::JobSpec {
                program,
                arguments,
                environment,
                layers: meticulous_base::nonempty![$(digest!($digest)),*],
                devices: meticulous_base::EnumSet::EMPTY,
                mounts: vec![],
                enable_loopback: false,
                working_directory: std::path::PathBuf::from("/"),
            }
        }
    }
}

#[macro_export]
macro_rules! result {
    [1] => {
        Ok(meticulous_base::JobSuccess {
            status: meticulous_base::JobStatus::Exited(0),
            stdout: meticulous_base::JobOutputResult::None,
            stderr: meticulous_base::JobOutputResult::None,
        })
    };
    [2] => {
        Ok(meticulous_base::JobSuccess {
            status: meticulous_base::JobStatus::Exited(1),
            stdout: meticulous_base::JobOutputResult::None,
            stderr: meticulous_base::JobOutputResult::None,
        })
    };
    [3] => {
        Ok(meticulous_base::JobSuccess {
            status: meticulous_base::JobStatus::Signaled(15),
            stdout: meticulous_base::JobOutputResult::None,
            stderr: meticulous_base::JobOutputResult::None,
        })
    };
    [$n:expr] => {
        Ok(meticulous_base::JobSuccess {
            status: meticulous_base::JobStatus::Exited($n),
            stdout: meticulous_base::JobOutputResult::None,
            stderr: meticulous_base::JobOutputResult::None,
        })
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

#[macro_export]
macro_rules! boxed_u8 {
    ($n:literal) => {
        $n.to_vec().into_boxed_slice()
    };
}
