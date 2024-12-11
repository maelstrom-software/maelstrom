//! Provide utilities for evaluating job specification directives.
//!
//! The job specification directives for `cargo-maelstrom` and the CLI differ in a number of ways, but
//! also have a number of similar constructs. This module includes utilities for those similar
//! constructs.

pub mod substitute;

use crate::{proto, IntoProtoBuf, TryFromProtoBuf};
use anyhow::{Error, Result};
use derive_more::From;
use enumset::{EnumSet, EnumSetType};
use maelstrom_base::{
    enum_set, GroupId, JobMount, JobNetwork, JobRootOverlay, JobTty, Timeout, UserId, Utf8PathBuf,
};
use maelstrom_util::template::{replace_template_vars, TemplateVars};
use serde::{de, Deserialize, Deserializer, Serialize};
use std::{
    collections::{BTreeMap, HashMap},
    env::{self, VarError},
    path::PathBuf,
    result,
    time::Duration,
};
use tuple::Map as _;

/// A function that can passed to [`substitute::substitute`] as the `env_lookup` closure that will
/// resolve variables from the program's environment.
pub fn std_env_lookup(var: &str) -> Result<Option<String>> {
    match env::var(var) {
        Ok(val) => Ok(Some(val)),
        Err(VarError::NotPresent) => Ok(None),
        Err(err) => Err(Error::new(err)),
    }
}

/// A function used when writing customer deserializers for job specification directives to
/// indicate that two fields are incompatible.
pub fn incompatible<T, E>(field: &Option<T>, msg: &str) -> result::Result<(), E>
where
    E: de::Error,
{
    if field.is_some() {
        Err(E::custom(format_args!("{}", msg)))
    } else {
        Ok(())
    }
}

#[derive(Clone, Debug, Eq, Hash, IntoProtoBuf, Ord, PartialEq, PartialOrd, TryFromProtoBuf)]
#[proto(proto_buf_type = "proto::ImageRef")]
pub struct ImageRef {
    pub name: String,
    pub r#use: EnumSet<ImageUse>,
}

#[macro_export]
macro_rules! image_ref {
    (@expand [] -> [$name:expr, $layers:literal, $environment:literal, $working_directory:literal]) => {
        $crate::spec::ImageRef {
            name: $name.into(),
            r#use: {
                let mut r#use = ::maelstrom_base::EnumSet::new();
                if $layers {
                    r#use.insert($crate::spec::ImageUse::Layers);
                }
                if $environment {
                    r#use.insert($crate::spec::ImageUse::Environment);
                }
                if $working_directory {
                    r#use.insert($crate::spec::ImageUse::WorkingDirectory);
                }
                r#use
            },
        }
    };
    (@expand [all $(,$($field_in:ident),*)?] -> [$name:expr, $old_layers:literal, $environment:literal, $working_directory:literal]) => {
        $crate::image_ref!(@expand [] -> [$name, true, true, true])
    };
    (@expand [layers $(,$($field_in:ident),*)?] -> [$name:expr, $old_layers:literal, $environment:literal, $working_directory:literal]) => {
        $crate::image_ref!(@expand [$($($field_in),*)?] -> [$name, true, $environment, $working_directory])
    };
    (@expand [environment $(,$($field_in:ident),*)?] -> [$name:expr, $layers:literal, $old_environment:literal, $working_directory:literal]) => {
        $crate::image_ref!(@expand [$($($field_in),*)?] -> [$name, $layers, true, $working_directory])
    };
    (@expand [working_directory $(,$($field_in:ident),*)?] -> [$name:expr, $layers:literal, $environment:literal, $old_working_directory:literal]) => {
        $crate::image_ref!(@expand [$($($field_in),*)?] -> [$name, $layers, $environment, true])
    };
    ($name:expr $(, $($use:ident),+ $(,)?)?) => {
        $crate::image_ref!(@expand [$($($use),+)?] -> [$name, false, false, false])
    };
}

#[derive(Clone, Debug, Eq, Hash, IntoProtoBuf, Ord, PartialEq, PartialOrd, TryFromProtoBuf)]
#[proto(proto_buf_type = "proto::ContainerRef")]
pub struct ContainerRef {
    pub name: String,
    pub r#use: EnumSet<ContainerUse>,
}

#[macro_export]
macro_rules! container_ref {
    (@expand [] -> [
         name: $name:expr,
         layers: $layers:literal,
         root_overlay: $root_overlay:literal,
         environment: $environment:literal,
         working_directory: $working_directory:literal,
         mounts: $mounts:literal,
         network: $network:literal,
         user: $user:literal,
         group: $group:literal,
    ]) => {
        $crate::spec::ContainerRef {
            name: $name.into(),
            r#use: {
                let mut r#use = ::maelstrom_base::EnumSet::new();
                if $layers {
                    r#use.insert($crate::spec::ContainerUse::Layers);
                }
                if $root_overlay {
                    r#use.insert($crate::spec::ContainerUse::RootOverlay);
                }
                if $environment {
                    r#use.insert($crate::spec::ContainerUse::Environment);
                }
                if $working_directory {
                    r#use.insert($crate::spec::ContainerUse::WorkingDirectory);
                }
                if $mounts {
                    r#use.insert($crate::spec::ContainerUse::Mounts);
                }
                if $network {
                    r#use.insert($crate::spec::ContainerUse::Network);
                }
                if $user {
                    r#use.insert($crate::spec::ContainerUse::User);
                }
                if $group {
                    r#use.insert($crate::spec::ContainerUse::Group);
                }
                r#use
            },
        }
    };
    (@expand [all $(,$($field_in:ident),*)?] -> [
         name: $name:expr,
         layers: $layers:literal,
         root_overlay: $root_overlay:literal,
         environment: $environment:literal,
         working_directory: $working_directory:literal,
         mounts: $mounts:literal,
         network: $network:literal,
         user: $user:literal,
         group: $group:literal,
    ]) => {
        $crate::container_ref!(@expand [] -> [
            name: $name,
            layers: true,
            root_overlay: true,
            environment: true,
            working_directory: true,
            mounts: true,
            network: true,
            user: true,
            group: true,
        ])
    };
    (@expand [layers $(,$($field_in:ident),*)?] -> [
         name: $name:expr,
         layers: $layers:literal,
         root_overlay: $root_overlay:literal,
         environment: $environment:literal,
         working_directory: $working_directory:literal,
         mounts: $mounts:literal,
         network: $network:literal,
         user: $user:literal,
         group: $group:literal,
    ]) => {
        $crate::container_ref!(@expand [$($($field_in),*)?] -> [
            name: $name,
            layers: true,
            root_overlay: $root_overlay,
            environment: $environment,
            working_directory: $working_directory,
            mounts: $mounts,
            network: $network,
            user: $user,
            group: $group,
        ])
    };
    (@expand [root_overlay $(,$($field_in:ident),*)?] -> [
         name: $name:expr,
         layers: $layers:literal,
         root_overlay: $root_overlay:literal,
         environment: $environment:literal,
         working_directory: $working_directory:literal,
         mounts: $mounts:literal,
         network: $network:literal,
         user: $user:literal,
         group: $group:literal,
    ]) => {
        $crate::container_ref!(@expand [$($($field_in),*)?] -> [
            name: $name,
            layers: $layers,
            root_overlay: true,
            environment: $environment,
            working_directory: $working_directory,
            mounts: $mounts,
            network: $network,
            user: $user,
            group: $group,
        ])
    };
    (@expand [environment $(,$($field_in:ident),*)?] -> [
         name: $name:expr,
         layers: $layers:literal,
         root_overlay: $root_overlay:literal,
         environment: $environment:literal,
         working_directory: $working_directory:literal,
         mounts: $mounts:literal,
         network: $network:literal,
         user: $user:literal,
         group: $group:literal,
    ]) => {
        $crate::container_ref!(@expand [$($($field_in),*)?] -> [
            name: $name,
            layers: $layers,
            root_overlay: $root_overlay,
            environment: true,
            working_directory: $working_directory,
            mounts: $mounts,
            network: $network,
            user: $user,
            group: $group,
        ])
    };
    (@expand [working_directory $(,$($field_in:ident),*)?] -> [
         name: $name:expr,
         layers: $layers:literal,
         root_overlay: $root_overlay:literal,
         environment: $environment:literal,
         working_directory: $working_directory:literal,
         mounts: $mounts:literal,
         network: $network:literal,
         user: $user:literal,
         group: $group:literal,
    ]) => {
        $crate::container_ref!(@expand [$($($field_in),*)?] -> [
            name: $name,
            layers: $layers,
            root_overlay: $root_overlay,
            environment: $environment,
            working_directory: true,
            mounts: $mounts,
            network: $network,
            user: $user,
            group: $group,
        ])
    };
    (@expand [mounts $(,$($field_in:ident),*)?] -> [
         name: $name:expr,
         layers: $layers:literal,
         root_overlay: $root_overlay:literal,
         environment: $environment:literal,
         working_directory: $working_directory:literal,
         mounts: $mounts:literal,
         network: $network:literal,
         user: $user:literal,
         group: $group:literal,
    ]) => {
        $crate::container_ref!(@expand [$($($field_in),*)?] -> [
            name: $name,
            layers: $layers,
            root_overlay: $root_overlay,
            environment: $environment,
            working_directory: $working_directory,
            mounts: true,
            network: $network,
            user: $user,
            group: $group,
        ])
    };
    (@expand [network $(,$($field_in:ident),*)?] -> [
         name: $name:expr,
         layers: $layers:literal,
         root_overlay: $root_overlay:literal,
         environment: $environment:literal,
         working_directory: $working_directory:literal,
         mounts: $mounts:literal,
         network: $network:literal,
         user: $user:literal,
         group: $group:literal,
    ]) => {
        $crate::container_ref!(@expand [$($($field_in),*)?] -> [
            name: $name,
            layers: $layers,
            root_overlay: $root_overlay,
            environment: $environment,
            working_directory: $working_directory,
            mounts: $mounts,
            network: true,
            user: $user,
            group: $group,
        ])
    };
    (@expand [user $(,$($field_in:ident),*)?] -> [
         name: $name:expr,
         layers: $layers:literal,
         root_overlay: $root_overlay:literal,
         environment: $environment:literal,
         working_directory: $working_directory:literal,
         mounts: $mounts:literal,
         network: $network:literal,
         user: $user:literal,
         group: $group:literal,
    ]) => {
        $crate::container_ref!(@expand [$($($field_in),*)?] -> [
            name: $name,
            layers: $layers,
            root_overlay: $root_overlay,
            environment: $environment,
            working_directory: $working_directory,
            mounts: $mounts,
            network: $network,
            user: true,
            group: $group,
        ])
    };
    (@expand [group $(,$($field_in:ident),*)?] -> [
         name: $name:expr,
         layers: $layers:literal,
         root_overlay: $root_overlay:literal,
         environment: $environment:literal,
         working_directory: $working_directory:literal,
         mounts: $mounts:literal,
         network: $network:literal,
         user: $user:literal,
         group: $group:literal,
    ]) => {
        $crate::container_ref!(@expand [$($($field_in),*)?] -> [
            name: $name,
            layers: $layers,
            root_overlay: $root_overlay,
            environment: $environment,
            working_directory: $working_directory,
            mounts: $mounts,
            network: $network,
            user: $user,
            group: true,
        ])
    };
    ($name:expr $(, $($use:ident),+ $(,)?)?) => {
        $crate::container_ref!(@expand [$($($use),+)?] -> [
            name: $name,
            layers: false,
            root_overlay: false,
            environment: false,
            working_directory: false,
            mounts: false,
            network: false,
            user: false,
            group: false,
        ])
    };
}

#[derive(Clone, Debug, Eq, Hash, IntoProtoBuf, Ord, PartialEq, PartialOrd, TryFromProtoBuf)]
#[proto(
    proto_buf_type = "proto::ContainerParent",
    enum_type = "proto::container_parent::Parent"
)]
pub enum ContainerParent {
    Image(ImageRef),
    Container(ContainerRef),
}

#[macro_export]
macro_rules! image_container_parent {
    ($($arg:tt)*) => {
        $crate::spec::ContainerParent::Image($crate::image_ref!($($arg)*))
    };
}

#[macro_export]
macro_rules! container_container_parent {
    ($($arg:tt)*) => {
        $crate::spec::ContainerParent::Container($crate::container_ref!($($arg)*))
    };
}

#[derive(
    IntoProtoBuf,
    TryFromProtoBuf,
    Clone,
    Debug,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Serialize,
    Deserialize,
)]
#[proto(proto_buf_type = "proto::EnvironmentSpec")]
pub struct EnvironmentSpec {
    pub vars: BTreeMap<String, String>,
    pub extend: bool,
}

#[macro_export]
macro_rules! environment_spec {
    ($extend:expr $(, $($($key:expr => $value:expr),+ $(,)?)?)?) => {
        $crate::spec::EnvironmentSpec {
            extend: $extend.into(),
            vars: ::std::collections::BTreeMap::from([$($($(($key.into(), $value.into())),+)?)?]),
        }
    }
}

pub trait IntoEnvironment {
    fn into_environment(self) -> Vec<EnvironmentSpec>;
}

impl IntoEnvironment for Vec<EnvironmentSpec> {
    fn into_environment(self) -> Self {
        self
    }
}

impl<const N: usize> IntoEnvironment for [EnvironmentSpec; N] {
    fn into_environment(self) -> Vec<EnvironmentSpec> {
        self.into_iter().collect()
    }
}

impl IntoEnvironment for EnvironmentSpec {
    fn into_environment(self) -> Vec<Self> {
        vec![self]
    }
}

impl<KeyT, ValueT, const N: usize> IntoEnvironment for [(KeyT, ValueT); N]
where
    KeyT: Into<String>,
    ValueT: Into<String>,
{
    fn into_environment(self) -> Vec<EnvironmentSpec> {
        vec![EnvironmentSpec {
            vars: self
                .into_iter()
                .map(|(k, v)| (k.into(), v.into()))
                .collect(),
            extend: false,
        }]
    }
}

macro_rules! into_env_container {
    ($container:ty) => {
        impl<KeyT, ValueT> IntoEnvironment for $container
        where
            KeyT: Into<String>,
            ValueT: Into<String>,
        {
            fn into_environment(self) -> Vec<EnvironmentSpec> {
                vec![EnvironmentSpec {
                    vars: self
                        .into_iter()
                        .map(|(k, v)| (k.into(), v.into()))
                        .collect(),
                    extend: false,
                }]
            }
        }
    };
}

into_env_container!(Vec<(KeyT, ValueT)>);
into_env_container!(BTreeMap<KeyT, ValueT>);
into_env_container!(HashMap<KeyT, ValueT>);

pub fn environment_eval(
    inital_env: BTreeMap<String, String>,
    env: Vec<EnvironmentSpec>,
    env_lookup: impl Fn(&str) -> Result<Option<String>>,
) -> Result<Vec<String>> {
    fn substitute_environment(
        env_lookup: impl Fn(&str) -> Result<Option<String>>,
        prev: &BTreeMap<String, String>,
        new: &BTreeMap<String, String>,
    ) -> Result<Vec<(String, String)>> {
        new.iter()
            .map(|(k, v)| {
                substitute::substitute(v, &env_lookup, |var| prev.get(var).map(String::as_str))
                    .map(|v| (k.clone(), String::from(v)))
                    .map_err(Error::new)
            })
            .collect()
    }
    let mut running_env = inital_env;
    for entry in env {
        if entry.extend {
            running_env.extend(substitute_environment(
                &env_lookup,
                &running_env,
                &entry.vars,
            )?);
        } else {
            running_env = substitute_environment(&env_lookup, &running_env, &entry.vars)?
                .into_iter()
                .collect();
        }
    }
    Ok(running_env
        .into_iter()
        .map(|(key, value)| format!("{key}={value}"))
        .collect())
}

#[derive(IntoProtoBuf, TryFromProtoBuf, Clone, Debug, Default, Eq, Ord, PartialEq, PartialOrd)]
#[proto(proto_buf_type = "proto::ContainerSpec")]
pub struct ContainerSpec {
    pub parent: Option<ContainerParent>,
    pub layers: Vec<LayerSpec>,
    pub root_overlay: Option<JobRootOverlay>,
    pub environment: Vec<EnvironmentSpec>,
    pub working_directory: Option<Utf8PathBuf>,
    pub mounts: Vec<JobMount>,
    pub network: Option<JobNetwork>,
    pub user: Option<UserId>,
    pub group: Option<GroupId>,
}

#[macro_export]
macro_rules! container_spec {
    (@expand [] -> []) => {
        $crate::spec::ContainerSpec::default()
    };
    (@expand [] -> [$($fields:tt)+]) => {
        $crate::spec::ContainerSpec {
            $($fields)+,
            .. $crate::container_spec!(@expand [] -> [])
        }
    };
    (@expand [parent: $parent:expr $(,$($field_in:tt)*)?] -> [$($($field_out:tt)+)?]) => {
        $crate::container_spec!(@expand [$($($field_in)*)?] -> [$($($field_out)+,)? parent: Some($parent)])
    };
    (@expand [layers: $layers:expr $(,$($field_in:tt)*)?] -> [$($($field_out:tt)+)?]) => {
        $crate::container_spec!(@expand [$($($field_in)*)?] -> [$($($field_out)+,)? layers: $layers.into()])
    };
    (@expand [root_overlay: $root_overlay:expr $(,$($field_in:tt)*)?] -> [$($($field_out:tt)+)?]) => {
        $crate::container_spec!(@expand [$($($field_in)*)?] -> [$($($field_out)+,)? root_overlay: Some($root_overlay)])
    };
    (@expand [environment: $environment:expr $(,$($field_in:tt)*)?] -> [$($($field_out:tt)+)?]) => {
        $crate::container_spec!(@expand [$($($field_in)*)?] -> [$($($field_out)+,)? environment: $crate::spec::IntoEnvironment::into_environment($environment)])
    };
    (@expand [working_directory: $dir:expr $(,$($field_in:tt)*)?] -> [$($($field_out:tt)+)?]) => {
        $crate::container_spec!(@expand [$($($field_in)*)?] -> [$($($field_out)+,)? working_directory: Some($dir.into())])
    };
    (@expand [mounts: $mounts:expr $(,$($field_in:tt)*)?] -> [$($($field_out:tt)+)?]) => {
        $crate::container_spec!(@expand [$($($field_in)*)?] -> [$($($field_out)+,)? mounts: $mounts.into()])
    };
    (@expand [network: $network:expr $(,$($field_in:tt)*)?] -> [$($($field_out:tt)+)?]) => {
        $crate::container_spec!(@expand [$($($field_in)*)?] -> [$($($field_out)+,)? network: Some($network)])
    };
    (@expand [user: $user:expr $(,$($field_in:tt)*)?] -> [$($($field_out:tt)+)?]) => {
        $crate::container_spec!(@expand [$($($field_in)*)?] -> [$($($field_out)+,)? user: Some(::maelstrom_base::UserId::new($user))])
    };
    (@expand [group: $group:expr $(,$($field_in:tt)*)?] -> [$($($field_out:tt)+)?]) => {
        $crate::container_spec!(@expand [$($($field_in)*)?] -> [$($($field_out)+,)? group: Some(::maelstrom_base::GroupId::new($group))])
    };
    ($($field_in:tt)*) => {
        $crate::container_spec!(@expand [$($field_in)*] -> [])
    };
}

#[derive(IntoProtoBuf, TryFromProtoBuf, Clone, Debug, PartialEq, Eq)]
#[proto(proto_buf_type = "proto::JobSpec")]
pub struct JobSpec {
    #[proto(option)]
    pub container: ContainerSpec,
    pub program: Utf8PathBuf,
    pub arguments: Vec<String>,
    pub timeout: Option<Timeout>,
    pub estimated_duration: Option<Duration>,
    pub allocate_tty: Option<JobTty>,
    pub priority: i8,
}

#[macro_export]
macro_rules! job_spec {
    (@expand [$program:expr] [] -> [] [$($container_field:tt)*]) => {
        $crate::spec::JobSpec {
            container: $crate::container_spec!{$($container_field)*},
            program: $program.into(),
            arguments: Default::default(),
            timeout: Default::default(),
            estimated_duration: Default::default(),
            allocate_tty: Default::default(),
            priority: Default::default(),
        }
    };
    (@expand [$program:expr] [] -> [$($field:tt)+] [$($container_field:tt)*]) => {
        $crate::spec::JobSpec {
            $($field)+,
            .. $crate::job_spec!(@expand [$program] [] -> [] [$($container_field)*])
        }
    };

    (@expand [$program:expr] [arguments: $arguments:expr $(,$($field_in:tt)*)?] -> [$($($field_out:tt)+)?] [$($container_field:tt)*]) => {
        $crate::job_spec!(@expand [$program] [$($($field_in)*)?] ->
            [$($($field_out)+,)? arguments: $arguments.into_iter().map(Into::into).collect()] [$($container_field)*])
    };
    (@expand [$program:expr] [timeout: $timeout:expr $(,$($field_in:tt)*)?] -> [$($($field_out:tt)+)?] [$($container_field:tt)*]) => {
        $crate::job_spec!(@expand [$program] [$($($field_in)*)?] ->
            [$($($field_out)+,)? timeout: ::maelstrom_base::Timeout::new($timeout)] [$($container_field)*])
    };
    (@expand [$program:expr] [estimated_duration: $estimated_duration:expr $(,$($field_in:tt)*)?] -> [$($($field_out:tt)+)?] [$($container_field:tt)*]) => {
        $crate::job_spec!(@expand [$program] [$($($field_in)*)?] ->
            [$($($field_out)+,)? estimated_duration: Some($estimated_duration)] [$($container_field)*])
    };
    (@expand [$program:expr] [allocate_tty: $allocate_tty:expr $(,$($field_in:tt)*)?] -> [$($($field_out:tt)+)?] [$($container_field:tt)*]) => {
        $crate::job_spec!(@expand [$program] [$($($field_in)*)?] ->
            [$($($field_out)+,)? allocate_tty: Some($allocate_tty)] [$($container_field)*])
    };
    (@expand [$program:expr] [priority: $priority:expr $(,$($field_in:tt)*)?] -> [$($($field_out:tt)+)?] [$($container_field:tt)*]) => {
        $crate::job_spec!(@expand [$program] [$($($field_in)*)?] ->
            [$($($field_out)+,)? priority: $priority] [$($container_field)*])
    };

    (@expand [$program:expr] [$container_field_name:ident: $container_field_value:expr $(,$($field_in:tt)*)?] -> [$($field_out:tt)*] [$($($container_field:tt)+)?]) => {
        $crate::job_spec!(@expand [$program] [$($($field_in)*)?] ->
            [$($field_out)*] [$($($container_field)+,)? $container_field_name: $container_field_value])
    };

    ($program:expr $(,$($field_in:tt)*)?) => {
        $crate::job_spec!(@expand [$program] [$($($field_in)*)?] -> [] [])
    };
}

#[derive(
    IntoProtoBuf,
    TryFromProtoBuf,
    Clone,
    Debug,
    Default,
    Deserialize,
    Eq,
    Hash,
    Ord,
    PartialEq,
    PartialOrd,
    Serialize,
)]
#[proto(proto_buf_type = "proto::PrefixOptions")]
pub struct PrefixOptions {
    pub strip_prefix: Option<Utf8PathBuf>,
    pub prepend_prefix: Option<Utf8PathBuf>,
    #[serde(default)]
    pub canonicalize: bool,
    #[serde(default)]
    pub follow_symlinks: bool,
}

#[derive(
    IntoProtoBuf,
    TryFromProtoBuf,
    Clone,
    Debug,
    Default,
    Deserialize,
    Eq,
    Hash,
    Ord,
    PartialEq,
    PartialOrd,
    Serialize,
)]
#[proto(proto_buf_type = "proto::SymlinkSpec")]
pub struct SymlinkSpec {
    pub link: Utf8PathBuf,
    pub target: Utf8PathBuf,
}

#[derive(
    Clone,
    Debug,
    Deserialize,
    Eq,
    Hash,
    IntoProtoBuf,
    Ord,
    PartialEq,
    PartialOrd,
    Serialize,
    TryFromProtoBuf,
)]
#[proto(
    proto_buf_type = "proto::LayerSpec",
    enum_type = "proto::layer_spec::Spec"
)]
#[serde(untagged, deny_unknown_fields)]
pub enum LayerSpec {
    #[proto(proto_buf_type = proto::TarLayer)]
    Tar {
        #[serde(rename = "tar")]
        path: Utf8PathBuf,
    },
    #[proto(proto_buf_type = proto::GlobLayer)]
    Glob {
        glob: String,
        #[serde(flatten)]
        #[proto(option)]
        prefix_options: PrefixOptions,
    },
    #[proto(proto_buf_type = proto::PathsLayer)]
    Paths {
        paths: Vec<Utf8PathBuf>,
        #[serde(flatten)]
        #[proto(option)]
        prefix_options: PrefixOptions,
    },
    #[proto(proto_buf_type = proto::StubsLayer)]
    Stubs { stubs: Vec<String> },
    #[proto(proto_buf_type = proto::SymlinksLayer)]
    Symlinks { symlinks: Vec<SymlinkSpec> },
    #[proto(proto_buf_type = proto::SharedLibraryDependenciesLayer)]
    SharedLibraryDependencies {
        #[serde(rename = "shared_library_dependencies")]
        binary_paths: Vec<Utf8PathBuf>,
        #[serde(flatten)]
        #[proto(option)]
        prefix_options: PrefixOptions,
    },
}

impl LayerSpec {
    pub fn replace_template_vars(&mut self, vars: &TemplateVars) -> Result<()> {
        match self {
            Self::Tar { path } => *path = replace_template_vars(path.as_str(), vars)?.into(),
            Self::Glob { glob, .. } => *glob = replace_template_vars(glob, vars)?,
            Self::Paths { paths, .. } => {
                for path in paths {
                    *path = replace_template_vars(path.as_str(), vars)?.into();
                }
            }
            Self::Stubs { stubs, .. } => {
                for stub in stubs {
                    *stub = replace_template_vars(stub, vars)?;
                }
            }
            Self::Symlinks { symlinks } => {
                for SymlinkSpec { link, target } in symlinks {
                    *link = replace_template_vars(link.as_str(), vars)?.into();
                    *target = replace_template_vars(target.as_str(), vars)?.into();
                }
            }
            Self::SharedLibraryDependencies { binary_paths, .. } => {
                for path in binary_paths {
                    *path = replace_template_vars(path.as_str(), vars)?.into();
                }
            }
        }
        Ok(())
    }
}

/// An enum and struct (`EnumSet<ImageUse>`) used for deserializing "image use" statements in JSON,
/// TOML, or other similar formats. This allows users to specify things like
/// `use = ["layers", "environment"]` in TOML, or the equivalent in JSON.
///
/// See [`Image`].
#[derive(Debug, Deserialize, EnumSetType, IntoProtoBuf, Serialize, TryFromProtoBuf)]
#[serde(rename_all = "snake_case")]
#[enumset(serialize_repr = "list")]
#[proto(proto_buf_type = "proto::ImageUse")]
pub enum ImageUse {
    Layers,
    Environment,
    WorkingDirectory,
}

#[derive(Debug, Deserialize, EnumSetType, IntoProtoBuf, Serialize, TryFromProtoBuf)]
#[serde(rename_all = "snake_case")]
#[enumset(serialize_repr = "list")]
#[proto(proto_buf_type = "proto::ContainerUse")]
pub enum ContainerUse {
    Layers,
    RootOverlay,
    Environment,
    WorkingDirectory,
    Mounts,
    Network,
    User,
    Group,
}

/// A struct used for deserializing "image" statements in JSON, TOML, or other similar formats.
/// This allows the user to specify an image name and the parts of the image they want to use.
#[derive(Debug, PartialEq)]
pub struct Image {
    pub name: String,
    pub use_: EnumSet<ImageUse>,
}

#[derive(Deserialize)]
#[serde(untagged)]
enum ImageForDeserialize {
    AsString(String),
    AsStruct {
        name: String,
        #[serde(rename = "use", default = "use_default")]
        use_: EnumSet<ImageUse>,
    },
}

fn use_default() -> EnumSet<ImageUse> {
    enum_set! {ImageUse::Layers | ImageUse::Environment}
}

impl<'de> Deserialize<'de> for Image {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        ImageForDeserialize::deserialize(deserializer).map(|i| match i {
            ImageForDeserialize::AsString(name) => Image {
                name,
                use_: use_default(),
            },
            ImageForDeserialize::AsStruct { name, use_ } => Image { name, use_ },
        })
    }
}

/// A simple wrapper struct for the config of a local OCI image. This is used for dependency
/// injection for the other functions in this module.
#[derive(Default)]
pub struct ImageConfig {
    /// Local `PathBuf`s pointing to the various layer artifacts.
    pub layers: Vec<PathBuf>,

    /// Optional `PathBuf` in the container's namespace for the working directory.
    pub working_directory: Option<Utf8PathBuf>,

    /// Optional environment variables for the container, assumed to be in `VAR=value` format.
    pub environment: Option<Vec<String>>,
}

/// An enum that indicates whether a value is explicitly specified, or implicitly defined to be the
/// value inherited from an image.
#[derive(PartialEq, Eq, Debug, Deserialize)]
pub enum PossiblyImage<T> {
    /// The value comes from the corresponding value in the image.
    Image,

    /// The value is explicitly set, and doesn't come from the image.
    Explicit(T),
}

/// A convenience struct for extracting parts of an OCI image for use in a
/// [`maelstrom_base::JobSpec`].
#[derive(Clone)]
pub struct ConvertedImage {
    name: String,
    layers: Vec<PathBuf>,
    environment: Option<Vec<String>>,
    working_directory: Option<Utf8PathBuf>,
}

#[macro_export]
macro_rules! converted_image {
    (@expand [$name:expr] [] -> []) => {
        $crate::spec::ConvertedImage::new($name.into(), $crate::spec::ImageConfig::default())
    };
    (@expand [$name:expr] [] -> [$($field_out:tt)+]) => {
        $crate::spec::ConvertedImage::new($name.into(), $crate::spec::ImageConfig {
            $($field_out)+,
            .. $crate::spec::ImageConfig::default()
        })
    };
    (@expand [$name:expr] [layers: $layers:expr $(,$($field_in:tt)*)?] -> [$($($field_out:tt)+)?]) => {
        $crate::converted_image!(@expand [$name] [$($($field_in)*)?] -> [
            $($($field_out)+,)? layers: $layers.into_iter().map(Into::into).collect()
        ])
    };
    (@expand [$name:expr] [environment: $environment:expr $(,$($field_in:tt)*)?] -> [$($($field_out:tt)+)?]) => {
        $crate::converted_image!(@expand [$name] [$($($field_in)*)?] -> [
            $($($field_out)+,)? environment: Some($environment.into_iter().map(Into::into).collect())
        ])
    };
    (@expand [$name:expr] [working_directory: $working_directory:expr $(,$($field_in:tt)*)?] -> [$($($field_out:tt)+)?]) => {
        $crate::converted_image!(@expand [$name] [$($($field_in)*)?] -> [
            $($($field_out)+,)? working_directory: Some($working_directory.into())
        ])
    };
    ($name:expr $(,$($field_in:tt)*)?) => {
        $crate::converted_image!(@expand [$name] [$($($field_in)*)?] -> [])
    };
}

impl ConvertedImage {
    /// Create a new [`ConvertedImage`].
    pub fn new(name: &str, config: ImageConfig) -> Self {
        Self {
            name: name.into(),
            layers: config.layers,
            environment: config.environment,
            working_directory: config.working_directory,
        }
    }

    /// Return the image name.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Return an iterator of layers for the image. If there is no image, the iterator will be
    /// empty.
    pub fn layers(&self) -> Result<Vec<LayerSpec>, String> {
        self.layers
            .iter()
            .map(|p| {
                Ok(LayerSpec::Tar {
                    path: Utf8PathBuf::from_path_buf(p.to_owned()).map_err(|_| {
                        format!("image {} has a non-UTF-8 layer path {p:?}", self.name())
                    })?,
                })
            })
            .collect()
    }

    /// Return a [`BTreeMap`] of environment variables for the image. If the image doesn't have any
    /// environment variables, this will return an error.
    pub fn environment(&self) -> Result<BTreeMap<String, String>, String> {
        Ok(BTreeMap::from_iter(
            self.environment
                .iter()
                .flatten()
                .map(|var| {
                    var.split_once('=')
                        .ok_or_else(|| {
                            format!(
                                "image {} has an invalid environment variable {var}",
                                self.name(),
                            )
                        })
                        .map(|pair| pair.map(str::to_string))
                })
                .collect::<Result<Vec<_>, _>>()?,
        ))
    }

    /// Return the working directory for the image. If the image doesn't have a working directory,
    /// this will return an error.
    pub fn working_directory(&self) -> Option<Utf8PathBuf> {
        self.working_directory.clone()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use indoc::indoc;
    use maelstrom_test::{
        glob_layer, path_buf_vec, paths_layer, shared_library_dependencies_layer, string,
        string_vec, stubs_layer, symlinks_layer, tar_layer,
    };
    use maplit::btreemap;
    use std::{ffi::OsStr, os::unix::ffi::OsStrExt as _};

    #[test]
    fn std_env_lookup_good() {
        let var = "AN_ENVIRONMENT_VARIABLE_1";
        let val = "foobar";
        env::set_var(var, val);
        assert_eq!(std_env_lookup(var).unwrap(), Some(val.to_string()));
    }

    #[test]
    fn std_env_lookup_missing() {
        let var = "AN_ENVIRONMENT_VARIABLE_TO_DELETE";
        env::remove_var(var);
        assert_eq!(std_env_lookup(var).unwrap(), None);
    }

    #[test]
    fn std_env_lookup_error() {
        let var = "AN_ENVIRONMENT_VARIABLE_2";
        let val = unsafe { std::ffi::OsString::from_encoded_bytes_unchecked(vec![0xff]) };
        env::set_var(var, val);
        assert_eq!(
            format!("{}", std_env_lookup(var).unwrap_err()),
            r#"environment variable was not valid unicode: "\xFF""#
        );
    }

    fn images(name: &str) -> ImageConfig {
        match name {
            "image1" => ImageConfig {
                layers: path_buf_vec!["42", "43"],
                working_directory: Some("/foo".into()),
                environment: Some(string_vec!["FOO=image-foo", "BAZ=image-baz",]),
            },
            "empty" => Default::default(),
            "invalid-env" => ImageConfig {
                environment: Some(string_vec!["FOO"]),
                ..Default::default()
            },
            "invalid-layer-path" => ImageConfig {
                layers: vec![PathBuf::from(OsStr::from_bytes(b"\xff"))],
                ..Default::default()
            },
            _ => panic!("no container named {name} found"),
        }
    }

    #[test]
    fn good_image_option() {
        let io = ConvertedImage::new("image1", images("image1"));
        assert_eq!(io.name(), "image1");
        assert_eq!(
            Vec::from_iter(io.layers().unwrap()),
            vec![tar_layer!("42"), tar_layer!("43")],
        );
        assert_eq!(
            io.environment().unwrap(),
            BTreeMap::from([
                (string!("BAZ"), string!("image-baz")),
                (string!("FOO"), string!("image-foo")),
            ]),
        );
        assert_eq!(io.working_directory().unwrap(), PathBuf::from("/foo"));
    }

    #[test]
    fn image_option_no_environment_and_no_working_directory() {
        let io = ConvertedImage::new("empty", images("empty"));
        assert_eq!(io.environment().unwrap(), BTreeMap::default());
        assert_eq!(io.working_directory(), None);
    }

    #[test]
    fn image_option_invalid_environment_variable() {
        let io = ConvertedImage::new("invalid-env", images("invalid-env"));
        assert_eq!(
            io.environment().unwrap_err(),
            "image invalid-env has an invalid environment variable FOO",
        );
    }

    #[test]
    fn image_option_invalid_layer_path() {
        let io = ConvertedImage::new("invalid-layer-path", images("invalid-layer-path"));
        assert_eq!(
            io.layers().unwrap_err(),
            r#"image invalid-layer-path has a non-UTF-8 layer path "\xFF""#,
        );
    }

    fn env_test(
        inital_env: BTreeMap<&'static str, &'static str>,
        input: Vec<(BTreeMap<&'static str, &'static str>, bool)>,
        expected: Vec<&'static str>,
    ) {
        let test_env: BTreeMap<String, String> = btreemap! {
            "FOO".into() => "bar".into(),
        };
        let res = environment_eval(
            inital_env
                .into_iter()
                .map(|(k, v)| (k.to_owned(), v.to_owned()))
                .collect(),
            input
                .into_iter()
                .map(|(vars, extend)| EnvironmentSpec {
                    vars: vars
                        .into_iter()
                        .map(|(k, v)| (k.to_owned(), v.to_owned()))
                        .collect(),
                    extend,
                })
                .collect(),
            |k| Ok(test_env.get(k).cloned()),
        )
        .unwrap();
        assert_eq!(
            res,
            Vec::from_iter(expected.into_iter().map(ToOwned::to_owned))
        );
    }

    #[test]
    fn environment_eval_inital_env_extend() {
        env_test(
            btreemap! {"BIN" => "bin" },
            vec![(btreemap! { "FOO" => "$env{FOO}", "BAR" => "baz" }, true)],
            vec!["BAR=baz", "BIN=bin", "FOO=bar"],
        )
    }

    #[test]
    fn environment_eval_inital_env_no_extend() {
        env_test(
            btreemap! {"BIN" => "bin" },
            vec![(btreemap! { "FOO" => "$env{FOO}", "BAR" => "baz" }, false)],
            vec!["BAR=baz", "FOO=bar"],
        )
    }

    #[test]
    fn environment_eval_inital_not_substituted() {
        env_test(
            btreemap! {"BIN" => "$env{FOO}" },
            vec![(btreemap! { "BAR" => "baz" }, true)],
            vec!["BAR=baz", "BIN=$env{FOO}"],
        )
    }

    #[test]
    fn environment_eval_env() {
        env_test(
            btreemap! {},
            vec![(btreemap! { "FOO" => "$env{FOO}", "BAR" => "baz" }, false)],
            vec!["BAR=baz", "FOO=bar"],
        )
    }

    #[test]
    fn environment_eval_prev() {
        env_test(
            btreemap! {},
            vec![
                (btreemap! { "FOO" => "$env{FOO}", "BAR" => "baz" }, false),
                (btreemap! { "BAZ" => "$prev{FOO}" }, true),
            ],
            vec!["BAR=baz", "BAZ=bar", "FOO=bar"],
        )
    }

    #[test]
    fn environment_eval_env_extend_false() {
        env_test(
            btreemap! {},
            vec![
                (btreemap! { "FOO" => "$env{FOO}", "BAR" => "baz" }, false),
                (btreemap! { "BAZ" => "$prev{FOO}" }, false),
            ],
            vec!["BAZ=bar"],
        )
    }

    #[test]
    fn environment_eval_env_extend_false_mixed() {
        env_test(
            btreemap! {},
            vec![
                (btreemap! { "A" => "1" }, true),
                (btreemap! { "B" => "$prev{A}" }, false),
                (btreemap! { "C" => "$prev{B}" }, true),
                (btreemap! { "D" => "$prev{C}" }, false),
            ],
            vec!["D=1"],
        )
    }

    #[derive(Debug, Deserialize, PartialEq)]
    struct ImageContainer {
        image: Image,
    }

    impl ImageContainer {
        fn new(image: Image) -> Self {
            Self { image }
        }
    }

    fn parse_image_container(file: &str) -> ImageContainer {
        toml::from_str(file).unwrap()
    }

    #[test]
    fn image_deserialize() {
        assert_eq!(
            parse_image_container(indoc! {r#"
                [image]
                name = "name"
                use = [ "layers", "environment", "working_directory" ]
            "#}),
            ImageContainer::new(Image {
                name: "name".into(),
                use_: enum_set! {ImageUse::Layers | ImageUse::Environment | ImageUse::WorkingDirectory},
            })
        );
    }

    #[test]
    fn image_deserialize_no_use() {
        assert_eq!(
            parse_image_container(indoc! {r#"
                [image]
                name = "name"
            "#}),
            ImageContainer::new(Image {
                name: "name".into(),
                use_: enum_set! {ImageUse::Layers | ImageUse::Environment},
            })
        );
    }

    #[test]
    fn image_deserialize_as_string() {
        assert_eq!(
            parse_image_container(indoc! {r#"
                image = "name"
            "#}),
            ImageContainer::new(Image {
                name: "name".into(),
                use_: enum_set! {ImageUse::Layers | ImageUse::Environment},
            })
        );
    }

    #[track_caller]
    fn layer_spec_parse_test(toml: &str, expected: LayerSpec) {
        assert_eq!(toml::from_str::<LayerSpec>(toml).unwrap(), expected);
    }

    #[test]
    fn tar_layer_spec() {
        layer_spec_parse_test(
            r#"
            tar = "foo.tar"
            "#,
            tar_layer!("foo.tar"),
        );
    }

    #[test]
    fn glob_layer_spec() {
        layer_spec_parse_test(
            r#"
            glob = "foo*.bin"
            "#,
            glob_layer!("foo*.bin"),
        );
    }

    #[test]
    fn glob_layer_spec_with_prefix_options() {
        layer_spec_parse_test(
            r#"
            glob = "foo*.bin"
            strip_prefix = "a"
            prepend_prefix = "b"
            "#,
            glob_layer!("foo*.bin", strip_prefix = "a", prepend_prefix = "b"),
        );
    }

    #[test]
    fn paths_layer_spec() {
        layer_spec_parse_test(
            r#"
            paths = [ "/foo", "/bar" ]
            "#,
            paths_layer!(["/foo", "/bar"]),
        );
    }

    #[test]
    fn paths_layer_spec_with_prefix_options() {
        layer_spec_parse_test(
            r#"
            paths = [ "/foo", "/bar" ]
            strip_prefix = "a"
            prepend_prefix = "b"
            "#,
            paths_layer!(["/foo", "/bar"], strip_prefix = "a", prepend_prefix = "b"),
        );
    }

    #[test]
    fn stubs_layer_spec() {
        layer_spec_parse_test(
            r#"
            stubs = [ "/foo", "/{bar,baz}/" ]
            "#,
            stubs_layer!(["/foo", "/{bar,baz}/"]),
        );
    }

    #[test]
    fn symlinks_layer_spec() {
        layer_spec_parse_test(
            r#"
            symlinks = [ { link = "/symlink", target = "/target" } ]
            "#,
            symlinks_layer!(["/symlink" -> "/target"]),
        );
    }

    #[test]
    fn shared_library_dependencies_layer_spec() {
        layer_spec_parse_test(
            r#"
            shared_library_dependencies = [ "/foo", "/bar" ]
            "#,
            shared_library_dependencies_layer!(["/foo", "/bar"]),
        );
    }

    #[test]
    fn shared_library_dependencies_layer_spec_with_prefix_options() {
        layer_spec_parse_test(
            r#"
            shared_library_dependencies = [ "/foo", "/bar" ]
            strip_prefix = "a"
            prepend_prefix = "b"
            "#,
            shared_library_dependencies_layer!(
                ["/foo", "/bar"],
                strip_prefix = "a",
                prepend_prefix = "b",
            ),
        );
    }
}
