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
    [1, $type:ident] => {
        maelstrom_base::JobSpec::new(
            "test_1",
            maelstrom_base::nonempty![(digest!(1), maelstrom_base::ArtifactType::$type)]
        )
    };
    [2, $type:ident] => {
        maelstrom_base::JobSpec::new(
            "test_2",
            maelstrom_base::nonempty![(digest!(2), maelstrom_base::ArtifactType::$type)]
        ).arguments(["arg_1"])
    };
    [3, $type:ident] => {
        maelstrom_base::JobSpec::new(
            "test_3",
            maelstrom_base::nonempty![(digest!(3), maelstrom_base::ArtifactType::$type)]
        ).arguments(["arg_1", "arg_2"])
    };
    [4, $type:ident] => {
        maelstrom_base::JobSpec::new(
            "test_4",
            maelstrom_base::nonempty![(digest!(4), maelstrom_base::ArtifactType::$type)]
        ).arguments(["arg_1", "arg_2", "arg_3"])
    };
    [$n:literal, $type:ident] => {
        maelstrom_base::JobSpec::new(
            concat!("test_", stringify!($n)),
            maelstrom_base::nonempty![(digest!($n), maelstrom_base::ArtifactType::$type)]
        ).arguments(["arg_1"])
    };
    [$n:literal, [$(($digest:expr, $type:ident)),*]] => {
        {
            let mut spec = spec![$n, Tar];
            spec.layers = maelstrom_base::nonempty![
                $((digest!($digest), maelstrom_base::ArtifactType::$type)),*
            ];
            spec
        }
    }
}

#[macro_export]
macro_rules! result {
    [1] => {
        Ok(maelstrom_base::JobSuccess {
            status: maelstrom_base::JobStatus::Exited(0),
            stdout: maelstrom_base::JobOutputResult::None,
            stderr: maelstrom_base::JobOutputResult::None,
        })
    };
    [2] => {
        Ok(maelstrom_base::JobSuccess {
            status: maelstrom_base::JobStatus::Exited(1),
            stdout: maelstrom_base::JobOutputResult::None,
            stderr: maelstrom_base::JobOutputResult::None,
        })
    };
    [3] => {
        Ok(maelstrom_base::JobSuccess {
            status: maelstrom_base::JobStatus::Signaled(15),
            stdout: maelstrom_base::JobOutputResult::None,
            stderr: maelstrom_base::JobOutputResult::None,
        })
    };
    [$n:expr] => {
        Ok(maelstrom_base::JobSuccess {
            status: maelstrom_base::JobStatus::Exited($n),
            stdout: maelstrom_base::JobOutputResult::None,
            stderr: maelstrom_base::JobOutputResult::None,
        })
    };
}

#[macro_export]
macro_rules! digest {
    [$n:expr] => {
        maelstrom_base::Sha256Digest::from($n as u64)
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
        vec![$($crate::path_buf!($e)),*]
    };
}

#[macro_export]
macro_rules! utf8_path_buf {
    ($e:expr) => {
        maelstrom_base::Utf8PathBuf::from($e)
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
    ($n:literal) => {
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
