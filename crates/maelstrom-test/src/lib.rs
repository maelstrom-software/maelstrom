#[macro_export]
macro_rules! cjid {
    [$n:expr] => {
        maelstrom_base::ClientJobId::from($n)
    };
}

#[macro_export]
macro_rules! cid {
    [$n:expr] => { maelstrom_base::ClientId::from($n) };
}

#[macro_export]
macro_rules! wid {
    [$n:expr] => { maelstrom_base::WorkerId::from($n) };
}

#[macro_export]
macro_rules! mid {
    [$n:expr] => { maelstrom_base::MonitorId::from($n) };
}

#[macro_export]
macro_rules! jid {
    [$n:expr] => {
        jid!($n, $n)
    };
    [$cid:expr, $cjid:expr] => {
        maelstrom_base::JobId{cid: cid![$cid], cjid: cjid![$cjid]}
    };
}

#[macro_export]
macro_rules! spec {
    (1 $($field:tt)*) => {
        maelstrom_base::job_spec! {
            "test_1",
            [maelstrom_base::tar_digest!(1)]
            $($field)*
        }
    };
    (2 $($field:tt)*) => {
        maelstrom_base::job_spec! {
            "test_2",
            [maelstrom_base::tar_digest!(2)],
            arguments: ["arg_1"]
            $($field)*
        }
    };
    (3 $($field:tt)*) => {
        maelstrom_base::job_spec! {
            "test_3",
            [maelstrom_base::tar_digest!(3)],
            arguments: ["arg_1", "arg_2"]
            $($field)*
        }
    };
    (4 $($field:tt)*) => {
        maelstrom_base::job_spec! {
            "test_4",
            [maelstrom_base::tar_digest!(4)],
            arguments: ["arg_1", "arg_2", "arg_3"]
            $($field)*
        }
    };
    ($n:literal $($field:tt)*) => {
        maelstrom_base::job_spec! {
            concat!("test_", stringify!($n)),
            [maelstrom_base::tar_digest!($n)],
            arguments: ["arg_1"]
            $($field)*
        }
    };
}

#[macro_export]
macro_rules! completed {
    [1] => {
        maelstrom_base::JobCompleted {
            status: maelstrom_base::JobTerminationStatus::Exited(0),
            effects: maelstrom_base::JobEffects {
                stdout: maelstrom_base::JobOutputResult::None,
                stderr: maelstrom_base::JobOutputResult::None,
                duration: std::time::Duration::from_secs(1),
            }
        }
    };
    [2] => {
        maelstrom_base::JobCompleted {
            status: maelstrom_base::JobTerminationStatus::Exited(1),
            effects: maelstrom_base::JobEffects {
                stdout: maelstrom_base::JobOutputResult::None,
                stderr: maelstrom_base::JobOutputResult::None,
                duration: std::time::Duration::from_secs(1),
            }
        }
    };
    [3] => {
        maelstrom_base::JobCompleted {
            status: maelstrom_base::JobTerminationStatus::Signaled(15),
            effects: maelstrom_base::JobEffects {
                stdout: maelstrom_base::JobOutputResult::None,
                stderr: maelstrom_base::JobOutputResult::None,
                duration: std::time::Duration::from_secs(1),
            }
        }
    };
    [$n:expr] => {
        maelstrom_base::JobCompleted {
            status: maelstrom_base::JobTerminationStatus::Exited($n),
            effects: maelstrom_base::JobEffects {
                stdout: maelstrom_base::JobOutputResult::None,
                stderr: maelstrom_base::JobOutputResult::None,
                duration: std::time::Duration::from_secs(1),
            }
        }
    };
}

#[macro_export]
macro_rules! outcome {
    [1] => { maelstrom_base::JobOutcome::Completed(completed![1]) };
    [2] => { maelstrom_base::JobOutcome::Completed(completed![2]) };
    [3] => { maelstrom_base::JobOutcome::Completed(completed![3]) };
    [$n:expr] => { maelstrom_base::JobOutcome::Completed(completed![$n]) };
}

#[macro_export]
macro_rules! path_buf {
    ($e:expr) => {
        std::path::Path::new($e).to_path_buf()
    };
}

#[macro_export]
macro_rules! utf8_path_buf {
    ($e:expr) => {
        maelstrom_base::Utf8PathBuf::from($e)
    };
}

#[macro_export]
macro_rules! boxed_u8 {
    ($n:expr) => {
        $n.to_vec().into_boxed_slice()
    };
}

#[macro_export]
macro_rules! string {
    ($e:expr) => {
        $e.to_string()
    };
}

#[macro_export]
macro_rules! string_vec {
    [$($e:expr),*] => {
        vec![$($e.to_string()),*]
    };
    [$($e:expr),*,] => {
        vec![$($e.to_string()),*]
    };
}

#[macro_export]
macro_rules! millis {
    ($millis:expr) => {
        Duration::from_millis($millis)
    };
}
