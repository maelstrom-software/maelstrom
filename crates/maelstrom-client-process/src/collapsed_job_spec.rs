#![allow(dead_code)]

use maelstrom_base::{
    GroupId, JobMount, JobNetwork, JobRootOverlay, JobTty, Timeout, UserId, Utf8PathBuf,
};
use maelstrom_client_base::spec::{
    ContainerSpec, EnvironmentSpec, ImageRef, JobSpec as ClientJobSpec, LayerSpec,
};
use std::time::Duration;

#[derive(Debug, Eq, PartialEq)]
pub struct CollapsedJobSpec {
    pub image: Option<ImageRef>,
    pub layers: Vec<LayerSpec>,
    pub root_overlay: JobRootOverlay,
    pub environment: Vec<EnvironmentSpec>,
    pub working_directory: Option<Utf8PathBuf>,
    pub mounts: Vec<JobMount>,
    pub network: JobNetwork,
    pub user: Option<UserId>,
    pub group: Option<GroupId>,
    pub program: Utf8PathBuf,
    pub arguments: Vec<String>,
    pub timeout: Option<Timeout>,
    pub estimated_duration: Option<Duration>,
    pub allocate_tty: Option<JobTty>,
    pub priority: i8,
}

#[macro_export]
macro_rules! collapsed_job_spec {
    (@expand [$program:expr] [] -> []) => {
        $crate::collapsed_job_spec::CollapsedJobSpec {
            image: Default::default(),
            layers: Default::default(),
            root_overlay: Default::default(),
            environment: Default::default(),
            working_directory: Default::default(),
            mounts: Default::default(),
            network: Default::default(),
            user: Default::default(),
            group: Default::default(),
            program: $program.into(),
            arguments: Default::default(),
            timeout: Default::default(),
            estimated_duration: Default::default(),
            allocate_tty: Default::default(),
            priority: Default::default(),
        }
    };
    (@expand [$program:expr] [] -> [$($field:tt)+]) => {
        $crate::collapsed_job_spec::CollapsedJobSpec {
            $($field)+,
            .. collapsed_job_spec!(@expand [$program] [] -> [])
        }
    };

    (@expand [$program:expr] [image: $image:expr $(,$($field_in:tt)*)?] -> [$($($field_out:tt)+)?]) => {
        collapsed_job_spec!(@expand [$program] [$($($field_in)*)?] ->
            [$($($field_out)+,)? image: Some($image.into())])
    };
    (@expand [$program:expr] [layers: $layers:expr $(,$($field_in:tt)*)?] -> [$($($field_out:tt)+)?]) => {
        collapsed_job_spec!(@expand [$program] [$($($field_in)*)?] ->
            [$($($field_out)+,)? layers: $layers.into_iter().map(Into::into).collect()])
    };
    (@expand [$program:expr] [root_overlay: $root_overlay:expr $(,$($field_in:tt)*)?] -> [$($($field_out:tt)+)?]) => {
        collapsed_job_spec!(@expand [$program] [$($($field_in)*)?] ->
            [$($($field_out)+,)? root_overlay: $root_overlay.into()])
    };
    (@expand [$program:expr] [environment: $environment:expr $(,$($field_in:tt)*)?] -> [$($($field_out:tt)+)?]) => {
        collapsed_job_spec!(@expand [$program] [$($($field_in)*)?] ->
            [$($($field_out)+,)? environment: $environment.into_iter().map(Into::into).collect()])
    };
    (@expand [$program:expr] [working_directory: $working_directory:expr $(,$($field_in:tt)*)?] -> [$($($field_out:tt)+)?]) => {
        collapsed_job_spec!(@expand [$program] [$($($field_in)*)?] ->
            [$($($field_out)+,)? working_directory: Some($working_directory.into())])
    };
    (@expand [$program:expr] [mounts: $mounts:expr $(,$($field_in:tt)*)?] -> [$($($field_out:tt)+)?]) => {
        collapsed_job_spec!(@expand [$program] [$($($field_in)*)?] ->
            [$($($field_out)+,)? mounts: $mounts.into_iter().map(Into::into).collect()])
    };
    (@expand [$program:expr] [network: $network:expr $(,$($field_in:tt)*)?] -> [$($($field_out:tt)+)?]) => {
        collapsed_job_spec!(@expand [$program] [$($($field_in)*)?] ->
            [$($($field_out)+,)? network: $network.into()])
    };
    (@expand [$program:expr] [user: $user:expr $(,$($field_in:tt)*)?] -> [$($($field_out:tt)+)?]) => {
        collapsed_job_spec!(@expand [$program] [$($($field_in)*)?] ->
            [$($($field_out)+,)? user: Some($user.into())])
    };
    (@expand [$program:expr] [group: $group:expr $(,$($field_in:tt)*)?] -> [$($($field_out:tt)+)?]) => {
        collapsed_job_spec!(@expand [$program] [$($($field_in)*)?] ->
            [$($($field_out)+,)? group: Some($group.into())])
    };
    (@expand [$program:expr] [arguments: $arguments:expr $(,$($field_in:tt)*)?] -> [$($($field_out:tt)+)?]) => {
        collapsed_job_spec!(@expand [$program] [$($($field_in)*)?] ->
            [$($($field_out)+,)? arguments: $arguments.into_iter().map(Into::into).collect()])
    };
    (@expand [$program:expr] [timeout: $timeout:expr $(,$($field_in:tt)*)?] -> [$($($field_out:tt)+)?]) => {
        collapsed_job_spec!(@expand [$program] [$($($field_in)*)?] ->
            [$($($field_out)+,)? timeout: ::maelstrom_base::Timeout::new($timeout)])
    };
    (@expand [$program:expr] [estimated_duration: $estimated_duration:expr $(,$($field_in:tt)*)?] -> [$($($field_out:tt)+)?]) => {
        collapsed_job_spec!(@expand [$program] [$($($field_in)*)?] ->
            [$($($field_out)+,)? estimated_duration: Some($estimated_duration.into())])
    };
    (@expand [$program:expr] [allocate_tty: $allocate_tty:expr $(,$($field_in:tt)*)?] -> [$($($field_out:tt)+)?]) => {
        collapsed_job_spec!(@expand [$program] [$($($field_in)*)?] ->
            [$($($field_out)+,)? allocate_tty: Some($allocate_tty.into())])
    };
    (@expand [$program:expr] [priority: $priority:expr $(,$($field_in:tt)*)?] -> [$($($field_out:tt)+)?]) => {
        collapsed_job_spec!(@expand [$program] [$($($field_in)*)?] ->
            [$($($field_out)+,)? priority: $priority.into()])
    };

    ($program:expr $(,$($field_in:tt)*)?) => {
        collapsed_job_spec!(@expand [$program] [$($($field_in)*)?] -> [])
    };
}

impl CollapsedJobSpec {
    pub fn new(
        job_spec: ClientJobSpec,
        _container_resolver: impl Fn(&str) -> Option<&ContainerSpec>,
    ) -> Result<Self, String> {
        let ClientJobSpec {
            container:
                ContainerSpec {
                    parent: _,
                    layers,
                    root_overlay,
                    environment,
                    working_directory,
                    mounts,
                    network,
                    user,
                    group,
                },
            program,
            arguments,
            timeout,
            estimated_duration,
            allocate_tty,
            priority,
        } = job_spec;
        Ok(CollapsedJobSpec {
            image: None,
            layers,
            root_overlay,
            environment,
            working_directory,
            mounts,
            network,
            user,
            group,
            program,
            arguments,
            timeout,
            estimated_duration,
            allocate_tty,
            priority,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use maelstrom_base::{proc_mount, tmp_mount, WindowSize};
    use maelstrom_client_base::{environment_spec, job_spec};
    use maelstrom_test::{millis, tar_layer};

    #[test]
    fn program() {
        assert_eq!(
            CollapsedJobSpec::new(job_spec!("prog"), |_| None,),
            Ok(collapsed_job_spec!("prog")),
        );
    }

    #[test]
    fn layers() {
        assert_eq!(
            CollapsedJobSpec::new(
                job_spec! {
                    "prog",
                    layers: [tar_layer!("foo.tar")],
                },
                |_| None,
            ),
            Ok(collapsed_job_spec! {
                "prog",
                layers: [tar_layer!("foo.tar")],
            })
        );
    }

    #[test]
    fn root_overlay() {
        assert_eq!(
            CollapsedJobSpec::new(
                job_spec! {
                    "prog",
                    root_overlay: JobRootOverlay::Tmp,
                },
                |_| None,
            ),
            Ok(collapsed_job_spec! {
                "prog",
                root_overlay: JobRootOverlay::Tmp,
            })
        );
    }

    #[test]
    fn environment() {
        assert_eq!(
            CollapsedJobSpec::new(
                job_spec! {
                    "prog",
                    environment: [
                        environment_spec!{false, "FOO" => "foo", "BAR" => "bar"},
                        environment_spec!{true, "FOO" => "frob", "BAZ" => "baz"},
                    ],
                },
                |_| None,
            ),
            Ok(collapsed_job_spec! {
                "prog",
                environment: [
                    environment_spec!{false, "FOO" => "foo", "BAR" => "bar"},
                    environment_spec!{true, "FOO" => "frob", "BAZ" => "baz"},
                ],
            })
        );
    }

    #[test]
    fn working_directory() {
        assert_eq!(
            CollapsedJobSpec::new(
                job_spec! {
                    "prog",
                    working_directory: "/root",
                },
                |_| None,
            ),
            Ok(collapsed_job_spec! {
                "prog",
                working_directory: "/root",
            })
        );
    }

    #[test]
    fn mounts() {
        assert_eq!(
            CollapsedJobSpec::new(
                job_spec! {
                    "prog",
                    mounts: [
                        proc_mount!("/proc"),
                        tmp_mount!("/tmp"),
                    ],
                },
                |_| None,
            ),
            Ok(collapsed_job_spec! {
                "prog",
                mounts: [
                    proc_mount!("/proc"),
                    tmp_mount!("/tmp"),
                ],
            })
        );
    }

    #[test]
    fn network() {
        assert_eq!(
            CollapsedJobSpec::new(
                job_spec! {
                    "prog",
                    network: JobNetwork::Local,
                },
                |_| None,
            ),
            Ok(collapsed_job_spec! {
                "prog",
                network: JobNetwork::Local,
            })
        );
    }

    #[test]
    fn user() {
        assert_eq!(
            CollapsedJobSpec::new(
                job_spec! {
                    "prog",
                    user: 100,
                },
                |_| None,
            ),
            Ok(collapsed_job_spec! {
                "prog",
                user: 100,
            })
        );
    }

    #[test]
    fn group() {
        assert_eq!(
            CollapsedJobSpec::new(
                job_spec! {
                    "prog",
                    group: 101,
                },
                |_| None,
            ),
            Ok(collapsed_job_spec! {
                "prog",
                group: 101,
            })
        );
    }

    #[test]
    fn arguments() {
        assert_eq!(
            CollapsedJobSpec::new(
                job_spec! {
                    "prog",
                    arguments: ["arg1", "arg2"],
                },
                |_| None,
            ),
            Ok(collapsed_job_spec! {
                "prog",
                arguments: ["arg1", "arg2"],
            })
        );
    }

    #[test]
    fn timeout() {
        assert_eq!(
            CollapsedJobSpec::new(
                job_spec! {
                    "prog",
                    timeout: 10,
                },
                |_| None,
            ),
            Ok(collapsed_job_spec! {
                "prog",
                timeout: 10,
            })
        );
    }

    #[test]
    fn estimated_duration() {
        assert_eq!(
            CollapsedJobSpec::new(
                job_spec! {
                    "prog",
                    estimated_duration: millis!(100),
                },
                |_| None,
            ),
            Ok(collapsed_job_spec! {
                "prog",
                estimated_duration: millis!(100),
            })
        );
    }

    #[test]
    fn allocate_tty() {
        assert_eq!(
            CollapsedJobSpec::new(
                job_spec! {
                    "prog",
                    allocate_tty: JobTty::new(b"123456", WindowSize::new(50, 100)),
                },
                |_| None,
            ),
            Ok(collapsed_job_spec! {
                "prog",
                allocate_tty: JobTty::new(b"123456", WindowSize::new(50, 100)),
            })
        );
    }

    #[test]
    fn priority() {
        assert_eq!(
            CollapsedJobSpec::new(
                job_spec! {
                    "prog",
                    priority: 42,
                },
                |_| None,
            ),
            Ok(collapsed_job_spec! {
                "prog",
                priority: 42,
            })
        );
    }
}
