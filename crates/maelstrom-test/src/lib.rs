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
macro_rules! digest_hash_set {
    [$($e:expr),*] => {
        ::std::collections::HashSet::from_iter([$(maelstrom_base::digest!($e)),*])
    };
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
        vec![$($crate::path_buf!($e)),*]
    };
}

#[macro_export]
macro_rules! path_buf_nonempty {
    [$($e:expr),*] => {
        nonempty![$($crate::path_buf!($e)),*]
    };
}

#[macro_export]
macro_rules! utf8_path_buf {
    ($e:expr) => {
        maelstrom_base::Utf8PathBuf::from($e)
    };
}

#[macro_export]
macro_rules! non_root_utf8_path_buf {
    ($e:expr) => {
        maelstrom_base::NonRootUtf8PathBuf::try_from(maelstrom_base::Utf8PathBuf::from($e)).unwrap()
    };
}

#[macro_export]
macro_rules! utf8_path_buf_vec {
    [$($e:expr),*] => {
        vec![$($crate::utf8_path_buf!($e)),*]
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
macro_rules! string_nonempty {
    [$($e:expr),*] => {
        nonempty![$($e.to_string()),*]
    };
    [$($e:expr),*,] => {
        nonempty![$($e.to_string()),*]
    };
}

#[macro_export]
macro_rules! prefix_options {
    (@expand [] -> []) => {
        ::maelstrom_client::spec::PrefixOptions::default()
    };
    (@expand [] -> [$($fields:tt)+]) => {
        ::maelstrom_client::spec::PrefixOptions {
            $($fields)+,
            ..::maelstrom_client::spec::PrefixOptions::default()
        }
    };
    (@expand [
        strip_prefix = $strip_prefix:expr
        $(,$($tail:tt)*)?
    ] -> [$($($options:tt)+)?]) => {
        $crate::prefix_options!(@expand [$($($tail)*)?] -> [
            $($($options)+,)?
            strip_prefix: Some(::std::convert::Into::into($strip_prefix))
        ])
    };
    (@expand [
        prepend_prefix = $prepend_prefix:expr
        $(,$($tail:tt)*)?
    ] -> [$($($options:tt)+)?]) => {
        $crate::prefix_options!(@expand [$($($tail)*)?] -> [
            $($($options)+,)?
            prepend_prefix: Some(::std::convert::Into::into($prepend_prefix))
        ])
    };
    (@expand [
        $field:ident = $value:expr
        $(,$($tail:tt)*)?
    ] -> [$($($options:tt)+)?]) => {
        $crate::prefix_options!(@expand [$($($tail)*)?] -> [
            $($($options)+,)?
            $field: $value
        ])
    };
    ($($fields:tt)*) => {
        $crate::prefix_options!(@expand [$($fields)*] -> [])
    };
}

#[macro_export]
macro_rules! tar_layer {
    ($path:expr) => {
        ::maelstrom_client::spec::LayerSpec::Tar {
            path: ::maelstrom_base::Utf8PathBuf::from($path),
        }
    };
}

#[macro_export]
macro_rules! glob_layer {
    ($glob:expr $(,$($prefix_options:tt)*)?) => {
        ::maelstrom_client::spec::LayerSpec::Glob {
            glob: ::std::convert::Into::into($glob),
            prefix_options: $crate::prefix_options!($($($prefix_options)*)?),
        }
    };
}

#[macro_export]
macro_rules! paths_layer {
    ([$($($path:expr),+ $(,)?)?] $(,$($prefix_options:tt)*)?) => {
        ::maelstrom_client::spec::LayerSpec::Paths {
            paths: vec![$($(::std::convert::Into::into($path)),*)?],
            prefix_options: $crate::prefix_options!($($($prefix_options)*)?),
        }
    };
}

#[macro_export]
macro_rules! stubs_layer {
    ([$($($stubs:expr),+ $(,)?)?]) => {
        ::maelstrom_client::spec::LayerSpec::Stubs {
            stubs: vec![$($(::std::convert::Into::into($stubs)),*)?],
        }
    };
}

#[macro_export]
macro_rules! symlinks_layer {
    ([$($($link:literal -> $target:literal),+ $(,)?)?]) => {
        ::maelstrom_client::spec::LayerSpec::Symlinks {
            symlinks: vec![$($(
                ::maelstrom_client::spec::SymlinkSpec {
                    link: ::std::convert::Into::into($link),
                    target: ::std::convert::Into::into($target),
                }
            ),*)?],
        }
    };
}

#[macro_export]
macro_rules! shared_library_dependencies_layer {
    ([$($($path:expr),+ $(,)?)?] $(,$($prefix_options:tt)*)?) => {
        ::maelstrom_client::spec::LayerSpec::SharedLibraryDependencies {
            binary_paths: vec![$($(::std::convert::Into::into($path)),*)?],
            prefix_options: $crate::prefix_options!($($($prefix_options)*)?),
        }
    };
}

#[macro_export]
macro_rules! timeout {
    ($n:literal) => {
        maelstrom_base::Timeout::new($n)
    };
}

#[macro_export]
macro_rules! millis {
    ($millis:expr) => {
        Duration::from_millis($millis)
    };
}
