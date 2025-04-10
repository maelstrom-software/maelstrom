//! Provide utilities for evaluating job specification directives.
//!
//! The job specification directives for `cargo-maelstrom` and the CLI differ in a number of ways, but
//! also have a number of similar constructs. This module includes utilities for those similar
//! constructs.

pub mod substitute;

use crate::{proto, IntoProtoBuf, TryFromProtoBuf};
use anyhow::{Error, Result};
use enumset::{EnumSet, EnumSetType};
use maelstrom_base::{
    CaptureFileSystemChanges, GroupId, JobMount, JobMountForTomlAndJson, JobNetwork, JobTty,
    Timeout, UserId, Utf8PathBuf,
};
use maelstrom_util::template::TemplateVariables;
use serde::{
    de::{Deserializer, Error as _},
    Deserialize, Serialize,
    __private::de::{Content, ContentRefDeserializer},
};
use std::{
    collections::BTreeMap,
    env::{self, VarError},
    path::PathBuf,
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

#[derive(Clone, Debug, Eq, Hash, IntoProtoBuf, Ord, PartialEq, PartialOrd, TryFromProtoBuf)]
#[proto(proto_buf_type = "proto::ImageRef")]
pub struct ImageRef {
    pub name: String,
    pub r#use: EnumSet<ImageUse>,
}

#[macro_export]
macro_rules! image_ref {
    (@expand [] -> [$name:expr, $use:expr]) => {
        $crate::spec::ImageRef {
            name: $name.into(),
            r#use: $use,
        }
    };
    (@expand [all $(, $($field_in:tt)*)?] -> [$name:literal, $use:expr]) => {
        $crate::image_ref!(@expand [$($($field_in)*)?] -> [$name, ::maelstrom_base::EnumSet::all()])
    };
    (@expand [layers $(, $($field_in:tt)*)?] -> [$name:literal, $use:expr]) => {
        $crate::image_ref!(@expand [$($($field_in)*)?] -> [$name, $use | $crate::spec::ImageUse::Layers])
    };
    (@expand [-layers $(, $($field_in:tt)*)?] -> [$name:literal, $use:expr]) => {
        $crate::image_ref!(@expand [$($($field_in)*)?] -> [$name, $use - $crate::spec::ImageUse::Layers])
    };
    (@expand [environment $(, $($field_in:tt)*)?] -> [$name:literal, $use:expr]) => {
        $crate::image_ref!(@expand [$($($field_in)*)?] -> [$name, $use | $crate::spec::ImageUse::Environment])
    };
    (@expand [-environment $(, $($field_in:tt)*)?] -> [$name:literal, $use:expr]) => {
        $crate::image_ref!(@expand [$($($field_in)*)?] -> [$name, $use - $crate::spec::ImageUse::Environment])
    };
    (@expand [working_directory $(, $($field_in:tt)*)?] -> [$name:literal, $use:expr]) => {
        $crate::image_ref!(@expand [$($($field_in)*)?] -> [$name, $use | $crate::spec::ImageUse::WorkingDirectory])
    };
    (@expand [-working_directory $(, $($field_in:tt)*)?] -> [$name:literal, $use:expr]) => {
        $crate::image_ref!(@expand [$($($field_in)*)?] -> [$name, $use - $crate::spec::ImageUse::WorkingDirectory])
    };
    ($name:literal $(, $($field:tt)*)?) => {
        $crate::image_ref!(@expand [$($($field)*)?] -> [$name, ::maelstrom_base::EnumSet::empty()])
    };
}

impl From<ImageRefWithImplicitOrExplicitUse> for ImageRef {
    fn from(image_ref: ImageRefWithImplicitOrExplicitUse) -> Self {
        Self {
            name: image_ref.name,
            r#use: image_ref.r#use.as_set(),
        }
    }
}

#[derive(Copy, Clone, Debug, PartialEq)]
pub enum ImplicitOrExplicitUse<T: EnumSetType> {
    Implicit,
    Explicit(EnumSet<T>),
}

impl<T: EnumSetType> ImplicitOrExplicitUse<T> {
    pub fn as_set(&self) -> EnumSet<T> {
        match self {
            Self::Implicit => EnumSet::all(),
            Self::Explicit(explicit) => *explicit,
        }
    }

    pub fn explicit(&self) -> EnumSet<T> {
        match self {
            Self::Implicit => EnumSet::empty(),
            Self::Explicit(explicit) => *explicit,
        }
    }
}

#[derive(Clone, Debug, Deserialize, PartialEq)]
#[serde(from = "ImageRefForDeserialize")]
pub struct ImageRefWithImplicitOrExplicitUse {
    pub name: String,
    pub r#use: ImplicitOrExplicitUse<ImageUse>,
}

impl From<ImageRefForDeserialize> for ImageRefWithImplicitOrExplicitUse {
    fn from(image: ImageRefForDeserialize) -> Self {
        match image {
            ImageRefForDeserialize::AsString(name)
            | ImageRefForDeserialize::AsStruct { name, r#use: None } => Self {
                name,
                r#use: ImplicitOrExplicitUse::Implicit,
            },
            ImageRefForDeserialize::AsStruct {
                name,
                r#use: Some(r#use),
            } => Self {
                name,
                r#use: ImplicitOrExplicitUse::Explicit(r#use),
            },
        }
    }
}

#[derive(Deserialize)]
#[serde(untagged)]
enum ImageRefForDeserialize {
    AsString(String),
    AsStruct {
        name: String,
        r#use: Option<EnumSet<ImageUse>>,
    },
}

#[derive(Clone, Debug, Eq, Hash, IntoProtoBuf, Ord, PartialEq, PartialOrd, TryFromProtoBuf)]
#[proto(proto_buf_type = "proto::ContainerRef")]
pub struct ContainerRef {
    pub name: String,
    pub r#use: EnumSet<ContainerUse>,
}

#[macro_export]
macro_rules! container_ref {
    (@expand [] -> [$name:expr, $use:expr]) => {
        $crate::spec::ContainerRef {
            name: $name.into(),
            r#use: $use,
        }
    };
    (@expand [all $(, $($field_in:tt)*)?] -> [$name:literal, $use:expr]) => {
        $crate::container_ref!(@expand [$($($field_in)*)?] -> [$name, ::maelstrom_base::EnumSet::all()])
    };
    (@expand [layers $(, $($field_in:tt)*)?] -> [$name:literal, $use:expr]) => {
        $crate::container_ref!(@expand [$($($field_in)*)?] -> [$name, $use | $crate::spec::ContainerUse::Layers])
    };
    (@expand [-layers $(, $($field_in:tt)*)?] -> [$name:literal, $use:expr]) => {
        $crate::container_ref!(@expand [$($($field_in)*)?] -> [$name, $use - $crate::spec::ContainerUse::Layers])
    };
    (@expand [enable_writable_file_system $(, $($field_in:tt)*)?] -> [$name:literal, $use:expr]) => {
        $crate::container_ref!(@expand [$($($field_in)*)?] -> [$name, $use | $crate::spec::ContainerUse::EnableWritableFileSystem])
    };
    (@expand [-enable_writable_file_system $(, $($field_in:tt)*)?] -> [$name:literal, $use:expr]) => {
        $crate::container_ref!(@expand [$($($field_in)*)?] -> [$name, $use - $crate::spec::ContainerUse::EnableWritableFileSystem])
    };
    (@expand [environment $(, $($field_in:tt)*)?] -> [$name:literal, $use:expr]) => {
        $crate::container_ref!(@expand [$($($field_in)*)?] -> [$name, $use | $crate::spec::ContainerUse::Environment])
    };
    (@expand [-environment $(, $($field_in:tt)*)?] -> [$name:literal, $use:expr]) => {
        $crate::container_ref!(@expand [$($($field_in)*)?] -> [$name, $use - $crate::spec::ContainerUse::Environment])
    };
    (@expand [working_directory $(, $($field_in:tt)*)?] -> [$name:literal, $use:expr]) => {
        $crate::container_ref!(@expand [$($($field_in)*)?] -> [$name, $use | $crate::spec::ContainerUse::WorkingDirectory])
    };
    (@expand [-working_directory $(, $($field_in:tt)*)?] -> [$name:literal, $use:expr]) => {
        $crate::container_ref!(@expand [$($($field_in)*)?] -> [$name, $use - $crate::spec::ContainerUse::WorkingDirectory])
    };
    (@expand [mounts $(, $($field_in:tt)*)?] -> [$name:literal, $use:expr]) => {
        $crate::container_ref!(@expand [$($($field_in)*)?] -> [$name, $use | $crate::spec::ContainerUse::Mounts])
    };
    (@expand [-mounts $(, $($field_in:tt)*)?] -> [$name:literal, $use:expr]) => {
        $crate::container_ref!(@expand [$($($field_in)*)?] -> [$name, $use - $crate::spec::ContainerUse::Mounts])
    };
    (@expand [network $(, $($field_in:tt)*)?] -> [$name:literal, $use:expr]) => {
        $crate::container_ref!(@expand [$($($field_in)*)?] -> [$name, $use | $crate::spec::ContainerUse::Network])
    };
    (@expand [-network $(, $($field_in:tt)*)?] -> [$name:literal, $use:expr]) => {
        $crate::container_ref!(@expand [$($($field_in)*)?] -> [$name, $use - $crate::spec::ContainerUse::Network])
    };
    (@expand [user $(, $($field_in:tt)*)?] -> [$name:literal, $use:expr]) => {
        $crate::container_ref!(@expand [$($($field_in)*)?] -> [$name, $use | $crate::spec::ContainerUse::User])
    };
    (@expand [-user $(, $($field_in:tt)*)?] -> [$name:literal, $use:expr]) => {
        $crate::container_ref!(@expand [$($($field_in)*)?] -> [$name, $use - $crate::spec::ContainerUse::User])
    };
    (@expand [group $(, $($field_in:tt)*)?] -> [$name:literal, $use:expr]) => {
        $crate::container_ref!(@expand [$($($field_in)*)?] -> [$name, $use | $crate::spec::ContainerUse::Group])
    };
    (@expand [-group $(, $($field_in:tt)*)?] -> [$name:literal, $use:expr]) => {
        $crate::container_ref!(@expand [$($($field_in)*)?] -> [$name, $use - $crate::spec::ContainerUse::Group])
    };
    ($name:literal $(, $($field:tt)*)?) => {
        $crate::container_ref!(@expand [$($($field)*)?] -> [$name, ::maelstrom_base::EnumSet::empty()])
    };
}

impl From<ContainerRefWithImplicitOrExplicitUse> for ContainerRef {
    fn from(image_ref: ContainerRefWithImplicitOrExplicitUse) -> Self {
        Self {
            name: image_ref.name,
            r#use: image_ref.r#use.as_set(),
        }
    }
}

#[derive(Clone, Deserialize)]
#[serde(from = "ContainerRefForDeserialize")]
pub struct ContainerRefWithImplicitOrExplicitUse {
    pub name: String,
    pub r#use: ImplicitOrExplicitUse<ContainerUse>,
}

impl From<ContainerRefForDeserialize> for ContainerRefWithImplicitOrExplicitUse {
    fn from(image: ContainerRefForDeserialize) -> Self {
        match image {
            ContainerRefForDeserialize::AsString(name)
            | ContainerRefForDeserialize::AsStruct { name, r#use: None } => Self {
                name,
                r#use: ImplicitOrExplicitUse::Implicit,
            },
            ContainerRefForDeserialize::AsStruct {
                name,
                r#use: Some(r#use),
            } => Self {
                name,
                r#use: ImplicitOrExplicitUse::Explicit(r#use),
            },
        }
    }
}

#[derive(Deserialize)]
#[serde(untagged)]
enum ContainerRefForDeserialize {
    AsString(String),
    AsStruct {
        name: String,
        r#use: Option<EnumSet<ContainerUse>>,
    },
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
#[proto(proto_buf_type = "proto::EnvironmentSpec")]
pub struct EnvironmentSpec {
    pub vars: BTreeMap<String, String>,
    pub extend: bool,
}

#[macro_export]
macro_rules! environment_spec {
    (@expand [] -> [$extend:expr] $vars:expr) => {
        $crate::spec::EnvironmentSpec {
            extend: $extend,
            vars: ::std::collections::BTreeMap::from($vars),
        }
    };
    (@expand [extend: $extend:expr $(, $($rest:tt)*)?] -> [$old_extend:expr] [$($vars:tt)*]) => {
        $crate::environment_spec!(@expand [$($($rest)*)?] -> [$extend.into()] [$($vars)*])
    };
    (@expand [$key:expr => $value:expr $(, $($rest:tt)*)?] -> [$extend:expr] [$($($vars:tt)+)?]) => {
        $crate::environment_spec!(@expand [$($($rest)*)?] -> [$extend] [$($($vars)+ ,)? ($key.into(), $value.into())])
    };
    ($($args:tt)*) => {
        $crate::environment_spec!(@expand [$($args)*] -> [true] [])
    };
}

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

#[derive(
    Clone,
    Debug,
    Default,
    Deserialize,
    Eq,
    IntoProtoBuf,
    Ord,
    PartialEq,
    PartialOrd,
    TryFromProtoBuf,
)]
#[proto(proto_buf_type = "proto::ContainerSpec")]
#[serde(try_from = "ContainerSpecForTomlAndJson")]
pub struct ContainerSpec {
    pub parent: Option<ContainerParent>,
    pub layers: Vec<LayerSpec>,
    pub environment: Vec<EnvironmentSpec>,
    pub mounts: Vec<JobMount>,
    pub network: Option<JobNetwork>,
    pub enable_writable_file_system: Option<bool>,
    pub working_directory: Option<Utf8PathBuf>,
    pub user: Option<UserId>,
    pub group: Option<GroupId>,
}

#[macro_export]
macro_rules! container_spec {
    (@expand [] -> [$($($fields:tt)+)?]) => {
        $crate::spec::ContainerSpec {
            $($($fields)+,)?
            .. $crate::spec::ContainerSpec::default()
        }
    };
    (@expand [parent: $parent:expr $(,$($field_in:tt)*)?] -> [$($($field_out:tt)+)?]) => {
        $crate::container_spec!(@expand [$($($field_in)*)?] -> [$($($field_out)+,)? parent: Some($parent)])
    };
    (@expand [layers: $layers:expr $(,$($field_in:tt)*)?] -> [$($($field_out:tt)+)?]) => {
        $crate::container_spec!(@expand [$($($field_in)*)?] -> [$($($field_out)+,)? layers: $layers.into()])
    };
    (@expand [environment: $environment:expr $(,$($field_in:tt)*)?] -> [$($($field_out:tt)+)?]) => {
        $crate::container_spec!(@expand [$($($field_in)*)?] -> [$($($field_out)+,)? environment: $environment.into_iter().collect()])
    };
    (@expand [working_directory: $dir:expr $(,$($field_in:tt)*)?] -> [$($($field_out:tt)+)?]) => {
        $crate::container_spec!(@expand [$($($field_in)*)?] -> [$($($field_out)+,)? working_directory: Some($dir.into())])
    };
    (@expand [enable_writable_file_system: $enable_writable_file_system:expr $(,$($field_in:tt)*)?] -> [$($($field_out:tt)+)?]) => {
        $crate::container_spec!(@expand [$($($field_in)*)?] -> [$($($field_out)+,)? enable_writable_file_system: Some($enable_writable_file_system)])
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

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ContainerSpecForTomlAndJson {
    pub image: Option<ImageRefWithImplicitOrExplicitUse>,
    pub parent: Option<ContainerRefWithImplicitOrExplicitUse>,
    pub layers: Option<Vec<LayerSpec>>,
    pub added_layers: Option<Vec<LayerSpec>>,
    pub environment: Option<EnvSelector>,
    pub added_environment: Option<EnvSelector>,
    pub working_directory: Option<Utf8PathBuf>,
    pub enable_writable_file_system: Option<bool>,
    pub mounts: Option<Vec<JobMountForTomlAndJson>>,
    pub added_mounts: Option<Vec<JobMountForTomlAndJson>>,
    pub network: Option<JobNetwork>,
    pub user: Option<UserId>,
    pub group: Option<GroupId>,
}

impl TryFrom<ContainerSpecForTomlAndJson> for ContainerSpec {
    type Error = String;

    fn try_from(container: ContainerSpecForTomlAndJson) -> Result<Self, Self::Error> {
        let ContainerSpecForTomlAndJson {
            image,
            parent,
            layers,
            added_layers,
            environment,
            added_environment,
            working_directory,
            enable_writable_file_system,
            mounts,
            added_mounts,
            network,
            user,
            group,
        } = container;

        let mut to_remove_from_image_use = EnumSet::default();
        let mut to_remove_from_parent_use = EnumSet::default();

        if image.is_some() && parent.is_some() {
            return Err("both `image` and `parent` cannot be specified".into());
        }

        let layers = match (layers, added_layers, &image, &parent) {
            (None, None, _, _) => vec![],
            (Some(_), Some(_), _, _) => {
                return Err("field `added_layers` cannot be set with `layers` field".into());
            }
            (_, _, Some(_), Some(_)) => {
                unreachable!();
            }
            (None, Some(_), None, None) => {
                return Err(concat!(
                    "field `added_layers` cannot be set without ",
                    "`image` or `parent` also being specified (try `layers` instead)",
                )
                .into());
            }
            (None, Some(added_layers), Some(image), None) => {
                if !image.r#use.as_set().contains(ImageUse::Layers) {
                    return Err(concat!(
                        "field `added_layers` requires `image` being specified ",
                        "with a `use` of `layers` (try `layers` instead)",
                    )
                    .into());
                }
                added_layers
            }
            (None, Some(added_layers), None, Some(parent)) => {
                if !parent.r#use.as_set().contains(ContainerUse::Layers) {
                    return Err(concat!(
                        "field `added_layers` requires `parent` being specified ",
                        "with a `use` of `layers` (try `layers` instead)",
                    )
                    .into());
                }
                added_layers
            }
            (Some(layers), None, None, None) => layers,
            (Some(layers), None, Some(image), None) => {
                if image.r#use.explicit().contains(ImageUse::Layers) {
                    return Err(concat!(
                        "field `layers` cannot be set if `image` with an explicit `use` of ",
                        "`layers` is also specified (try `added_layers` instead)",
                    )
                    .into());
                }
                to_remove_from_image_use.insert(ImageUse::Layers);
                layers
            }
            (Some(layers), None, None, Some(parent)) => {
                if parent.r#use.explicit().contains(ContainerUse::Layers) {
                    return Err(concat!(
                        "field `layers` cannot be set if `parent` with an explicit `use` of ",
                        "`layers` is also specified (try `added_layers` instead)",
                    )
                    .into());
                }
                to_remove_from_parent_use.insert(ContainerUse::Layers);
                layers
            }
        };

        let environment = match (environment, added_environment, &image, &parent) {
            (None, None, _, _) => vec![],
            (Some(_), Some(_), _, _) => {
                return Err(
                    "field `added_environment` cannot be set with `environment` field".into(),
                );
            }
            (_, _, Some(_), Some(_)) => {
                unreachable!();
            }
            (None, Some(_), None, None) => {
                return Err(concat!(
                    "field `added_environment` cannot be set without ",
                    "`image` or `parent` also being specified (try `environment` instead)",
                )
                .into());
            }
            (None, Some(added_environment), Some(image), None) => {
                if !image.r#use.as_set().contains(ImageUse::Environment) {
                    return Err(concat!(
                        "field `added_environment` requires `image` being specified ",
                        "with a `use` of `environment` (try `environment` instead)",
                    )
                    .into());
                }
                added_environment.into_environment_specs()
            }
            (None, Some(added_environment), None, Some(parent)) => {
                if !parent.r#use.as_set().contains(ContainerUse::Environment) {
                    return Err(concat!(
                        "field `added_environment` requires `parent` being specified ",
                        "with a `use` of `environment` (try `environment` instead)",
                    )
                    .into());
                }
                added_environment.into_environment_specs()
            }
            (Some(environment), None, None, None) => environment.into_environment_specs(),
            (Some(environment), None, Some(image), None) => {
                if image.r#use.explicit().contains(ImageUse::Environment) {
                    return Err(concat!(
                        "field `environment` cannot be set if `image` with an explicit `use` of ",
                        "`environment` is also specified (try `added_environment` instead)",
                    )
                    .into());
                }
                to_remove_from_image_use.insert(ImageUse::Environment);
                environment.into_environment_specs()
            }
            (Some(environment), None, None, Some(parent)) => {
                if parent.r#use.explicit().contains(ContainerUse::Environment) {
                    return Err(concat!(
                        "field `environment` cannot be set if `parent` with an explicit `use` of ",
                        "`environment` is also specified (try `added_environment` instead)",
                    )
                    .into());
                }
                to_remove_from_parent_use.insert(ContainerUse::Environment);
                environment.into_environment_specs()
            }
        };

        if working_directory.is_some() {
            if let Some(image) = &image {
                if image.r#use.explicit().contains(ImageUse::WorkingDirectory) {
                    return Err(concat!(
                        "field `working_directory` cannot be set if `image` with an ",
                        "explicit `use` of `working_directory` is also specified",
                    )
                    .into());
                }
                to_remove_from_image_use.insert(ImageUse::WorkingDirectory);
            }
            if let Some(parent) = &parent {
                if parent
                    .r#use
                    .explicit()
                    .contains(ContainerUse::WorkingDirectory)
                {
                    return Err(concat!(
                        "field `working_directory` cannot be set if `parent` with an ",
                        "explicit `use` of `working_directory` is also specified",
                    )
                    .into());
                }
                to_remove_from_parent_use.insert(ContainerUse::WorkingDirectory);
            }
        }

        if enable_writable_file_system.is_some() {
            if let Some(parent) = &parent {
                if parent
                    .r#use
                    .explicit()
                    .contains(ContainerUse::EnableWritableFileSystem)
                {
                    return Err(concat!(
                        "field `enable_writable_file_system` cannot be set if `parent` with an ",
                        "explicit `use` of `enable_writable_file_system` is also specified",
                    )
                    .into());
                }
                to_remove_from_parent_use.insert(ContainerUse::EnableWritableFileSystem);
            }
        }

        let mounts = match (mounts, added_mounts, &parent) {
            (None, None, _) => vec![],
            (Some(_), Some(_), _) => {
                return Err("field `added_mounts` cannot be set with `mounts` field".into());
            }
            (None, Some(_), None) => {
                return Err(concat!(
                    "field `added_mounts` cannot be set without ",
                    "`parent` also being specified (try `mounts` instead)",
                )
                .into());
            }
            (None, Some(added_mounts), Some(parent)) => {
                if !parent.r#use.as_set().contains(ContainerUse::Mounts) {
                    return Err(concat!(
                        "field `added_mounts` requires `parent` being specified ",
                        "with a `use` of `mounts` (try `mounts` instead)",
                    )
                    .into());
                }
                added_mounts
            }
            (Some(mounts), None, None) => mounts,
            (Some(mounts), None, Some(parent)) => {
                if parent.r#use.explicit().contains(ContainerUse::Mounts) {
                    return Err(concat!(
                        "field `mounts` cannot be set if `parent` with an explicit `use` of ",
                        "`mounts` is also specified (try `added_mounts` instead)",
                    )
                    .into());
                }
                to_remove_from_parent_use.insert(ContainerUse::Mounts);
                mounts
            }
        }
        .into_iter()
        .map(Into::into)
        .collect();

        if network.is_some() {
            if let Some(parent) = &parent {
                if parent.r#use.explicit().contains(ContainerUse::Network) {
                    return Err(concat!(
                        "field `network` cannot be set if `parent` with an ",
                        "explicit `use` of `network` is also specified",
                    )
                    .into());
                }
                to_remove_from_parent_use.insert(ContainerUse::Network);
            }
        }

        if user.is_some() {
            if let Some(parent) = &parent {
                if parent.r#use.explicit().contains(ContainerUse::User) {
                    return Err(concat!(
                        "field `user` cannot be set if `parent` with an ",
                        "explicit `use` of `user` is also specified",
                    )
                    .into());
                }
                to_remove_from_parent_use.insert(ContainerUse::User);
            }
        }

        if group.is_some() {
            if let Some(parent) = &parent {
                if parent.r#use.explicit().contains(ContainerUse::Group) {
                    return Err(concat!(
                        "field `group` cannot be set if `parent` with an ",
                        "explicit `use` of `group` is also specified",
                    )
                    .into());
                }
                to_remove_from_parent_use.insert(ContainerUse::Group);
            }
        }

        Ok(ContainerSpec {
            parent: match (image, parent) {
                (Some(image), _) => Some(ContainerParent::Image(ImageRef {
                    name: image.name,
                    r#use: image.r#use.as_set().difference(to_remove_from_image_use),
                })),
                (_, Some(parent)) => Some(ContainerParent::Container(ContainerRef {
                    name: parent.name,
                    r#use: parent.r#use.as_set().difference(to_remove_from_parent_use),
                })),
                (None, None) => None,
            },
            layers,
            enable_writable_file_system,
            environment,
            working_directory,
            mounts,
            network,
            user,
            group,
        })
    }
}

#[derive(Deserialize)]
#[serde(untagged)]
pub enum EnvSelector {
    Implicit(BTreeMap<String, String>),
    Explicit(Vec<EnvironmentSpec>),
}

impl EnvSelector {
    pub fn into_environment_specs(self) -> Vec<EnvironmentSpec> {
        match self {
            Self::Explicit(environment_specs) => environment_specs,
            Self::Implicit(vars) => {
                vec![EnvironmentSpec { extend: true, vars }]
            }
        }
    }
}

#[derive(Clone, Debug, Deserialize, Eq, IntoProtoBuf, PartialEq, TryFromProtoBuf)]
#[proto(proto_buf_type = "proto::JobSpec")]
#[serde(from = "JobSpecForTomlAndJson")]
pub struct JobSpec {
    #[proto(option)]
    pub container: ContainerSpec,
    pub program: Utf8PathBuf,
    pub arguments: Vec<String>,
    pub timeout: Option<Timeout>,
    pub estimated_duration: Option<Duration>,
    pub priority: i8,
    pub allocate_tty: Option<JobTty>,
    pub capture_file_system_changes: Option<CaptureFileSystemChanges>,
}

#[macro_export]
macro_rules! job_spec {
    (@expand [$program:expr] [] -> [$($($field:tt)+)?] [$($container_field:tt)*]) => {
        $crate::spec::JobSpec {
            $($($field)+,)?
            .. $crate::spec::JobSpec {
                container: $crate::container_spec!{$($container_field)*},
                program: $program.into(),
                arguments: Default::default(),
                timeout: Default::default(),
                estimated_duration: Default::default(),
                allocate_tty: Default::default(),
                priority: Default::default(),
                capture_file_system_changes: Default::default(),
            }
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
    (@expand [$program:expr] [capture_file_system_changes: $capture_file_system_changes:expr $(,$($field_in:tt)*)?] -> [$($($field_out:tt)+)?] [$($container_field:tt)*]) => {
        $crate::job_spec!(@expand [$program] [$($($field_in)*)?] ->
            [$($($field_out)+,)? capture_file_system_changes: Some($capture_file_system_changes)] [$($container_field)*])
    };

    (@expand [$program:expr] [$container_field_name:ident: $container_field_value:expr $(,$($field_in:tt)*)?] -> [$($field_out:tt)*] [$($($container_field:tt)+)?]) => {
        $crate::job_spec!(@expand [$program] [$($($field_in)*)?] ->
            [$($field_out)*] [$($($container_field)+,)? $container_field_name: $container_field_value])
    };

    ($program:expr $(,$($field_in:tt)*)?) => {
        $crate::job_spec!(@expand [$program] [$($($field_in)*)?] -> [] [])
    };
}

/// Currently, this is only used by maelstrom-run, though it seems like it's concieveable that it
/// may be used by other clients. Also, it's nice to have all of the parsing code in one place.
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct JobSpecForTomlAndJson {
    #[serde(flatten)]
    container: ContainerSpec,
    program: Utf8PathBuf,
    arguments: Option<Vec<String>>,
    timeout: Option<u32>,
    priority: Option<i8>,
}

impl From<JobSpecForTomlAndJson> for JobSpec {
    fn from(job_spec: JobSpecForTomlAndJson) -> Self {
        let JobSpecForTomlAndJson {
            container,
            program,
            arguments,
            timeout,
            priority,
        } = job_spec;
        JobSpec {
            container,
            program,
            arguments: arguments.unwrap_or_default(),
            timeout: timeout.and_then(Timeout::new),
            estimated_duration: None,
            allocate_tty: None,
            priority: priority.unwrap_or_default(),
            capture_file_system_changes: None,
        }
    }
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

#[macro_export]
macro_rules! prefix_options {
    (@expand [] -> [$($($fields:tt)+)?]) => {
        $crate::spec::PrefixOptions {
            $($($fields)+,)?
            .. $crate::spec::PrefixOptions::default()
        }
    };
    (@expand [strip_prefix: $strip_prefix:expr $(,$($field_in:tt)*)?] -> [$($($field_out:tt)+)?]) => {
        $crate::prefix_options!(@expand [$($($field_in)*)?] -> [$($($field_out)+,)? strip_prefix: Some($strip_prefix.into())])
    };
    (@expand [prepend_prefix: $prepend_prefix:expr $(,$($field_in:tt)*)?] -> [$($($field_out:tt)+)?]) => {
        $crate::prefix_options!(@expand [$($($field_in)*)?] -> [$($($field_out)+,)? prepend_prefix: Some($prepend_prefix.into())])
    };
    (@expand [canonicalize: $canonicalize:expr $(,$($field_in:tt)*)?] -> [$($($field_out:tt)+)?]) => {
        $crate::prefix_options!(@expand [$($($field_in)*)?] -> [$($($field_out)+,)? canonicalize: $canonicalize.into()])
    };
    (@expand [follow_symlinks: $follow_symlinks:expr $(,$($field_in:tt)*)?] -> [$($($field_out:tt)+)?]) => {
        $crate::prefix_options!(@expand [$($($field_in)*)?] -> [$($($field_out)+,)? follow_symlinks: $follow_symlinks.into()])
    };
    ($($field_in:tt)*) => {
        $crate::prefix_options!(@expand [$($field_in)*] -> [])
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
#[proto(proto_buf_type = "proto::SymlinkSpec")]
#[serde(deny_unknown_fields)]
pub struct SymlinkSpec {
    pub link: Utf8PathBuf,
    pub target: Utf8PathBuf,
}

#[macro_export]
macro_rules! symlink_spec {
    ($link:expr => $target:expr) => {
        $crate::spec::SymlinkSpec {
            link: $link.into(),
            target: $target.into(),
        }
    };
}

#[derive(
    Clone, Debug, Eq, Hash, IntoProtoBuf, Ord, PartialEq, PartialOrd, Serialize, TryFromProtoBuf,
)]
#[proto(
    proto_buf_type = "proto::LayerSpec",
    enum_type = "proto::layer_spec::Spec"
)]
#[serde(untagged, deny_unknown_fields)]
pub enum LayerSpec {
    Tar(TarLayerSpec),
    Glob(GlobLayerSpec),
    Paths(PathsLayerSpec),
    Stubs(StubsLayerSpec),
    Symlinks(SymlinksLayerSpec),
    SharedLibraryDependencies(SharedLibraryDependenciesLayerSpec),
    Command(CommandLayerSpec),
}

enum LayerSpecType {
    Tar,
    Glob,
    Paths,
    Stubs,
    Symlinks,
    SharedLibraryDependencies,
    Command,
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
#[proto(proto_buf_type = proto::TarLayer)]
#[serde(deny_unknown_fields)]
pub struct TarLayerSpec {
    #[serde(rename = "tar")]
    pub path: Utf8PathBuf,
}

#[macro_export]
macro_rules! tar_layer_spec {
    ($path:expr) => {
        $crate::spec::LayerSpec::Tar($crate::spec::TarLayerSpec { path: $path.into() })
    };
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
#[proto(proto_buf_type = proto::GlobLayer)]
#[serde(deny_unknown_fields)]
pub struct GlobLayerSpec {
    pub glob: String,
    #[serde(flatten)]
    #[proto(option)]
    pub prefix_options: PrefixOptions,
}

#[macro_export]
macro_rules! glob_layer_spec {
    ($glob:expr $(, $($prefix_option:tt)*)?) => {
        $crate::spec::LayerSpec::Glob($crate::spec::GlobLayerSpec {
            glob: $glob.into(),
            prefix_options: $crate::prefix_options!($($($prefix_option)*)?),
        })
    };
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
#[proto(proto_buf_type = proto::PathsLayer)]
#[serde(deny_unknown_fields)]
pub struct PathsLayerSpec {
    pub paths: Vec<Utf8PathBuf>,
    #[serde(flatten)]
    #[proto(option)]
    pub prefix_options: PrefixOptions,
}

#[macro_export]
macro_rules! paths_layer_spec {
    ($paths:expr $(, $($prefix_option:tt)*)?) => {
        $crate::spec::LayerSpec::Paths($crate::spec::PathsLayerSpec {
            paths: $paths.into_iter().map(Into::into).collect(),
            prefix_options: $crate::prefix_options!($($($prefix_option)*)?),
        })
    };
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
#[proto(proto_buf_type = proto::StubsLayer)]
#[serde(deny_unknown_fields)]
pub struct StubsLayerSpec {
    pub stubs: Vec<String>,
}

#[macro_export]
macro_rules! stubs_layer_spec {
    ($stubs:expr) => {
        $crate::spec::LayerSpec::Stubs($crate::spec::StubsLayerSpec {
            stubs: $stubs.into_iter().map(Into::into).collect(),
        })
    };
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
#[proto(proto_buf_type = proto::SymlinksLayer)]
#[serde(deny_unknown_fields)]
pub struct SymlinksLayerSpec {
    pub symlinks: Vec<SymlinkSpec>,
}

#[macro_export]
macro_rules! symlinks_layer_spec {
    ($symlinks:expr) => {
        $crate::spec::LayerSpec::Symlinks($crate::spec::SymlinksLayerSpec {
            symlinks: $symlinks.into_iter().map(Into::into).collect(),
        })
    };
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
#[proto(proto_buf_type = proto::SharedLibraryDependenciesLayer)]
#[serde(deny_unknown_fields)]
pub struct SharedLibraryDependenciesLayerSpec {
    #[serde(rename = "shared_library_dependencies")]
    pub binary_paths: Vec<Utf8PathBuf>,
    #[serde(flatten)]
    #[proto(option)]
    pub prefix_options: PrefixOptions,
}

#[macro_export]
macro_rules! shared_library_dependencies_layer_spec {
    ($binary_paths:expr $(, $($prefix_option:tt)*)?) => {
        $crate::spec::LayerSpec::SharedLibraryDependencies($crate::spec::SharedLibraryDependenciesLayerSpec {
            binary_paths: $binary_paths.into_iter().map(Into::into).collect(),
            prefix_options: $crate::prefix_options!($($($prefix_option)*)?),
        })
    };
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
#[proto(proto_buf_type = proto::CommandLayer)]
#[serde(deny_unknown_fields)]
pub struct CommandLayerSpec {
    pub command: Utf8PathBuf,
    #[serde(default)]
    pub arguments: Vec<String>,
}

#[macro_export]
macro_rules! command_layer_spec {
    ($command:expr $(,)?) => {
        $crate::spec::LayerSpec::Command($crate::spec::CommandLayerSpec {
            command: $command.into(),
            arguments: ::std::default::Default::default(),
        })
    };
    ($command:expr, $($arguments:tt)+) => {
        $crate::spec::LayerSpec::Command($crate::spec::CommandLayerSpec {
            command: $command.into(),
            arguments: [$($arguments)+].into_iter().map(Into::into).collect(),
        })
    };
}

impl<'de> Deserialize<'de> for LayerSpec {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let content = Content::deserialize(deserializer)?;
        let Content::Map(fields) = &content else {
            return Err(D::Error::custom(
                "expected a map in order to parse a layer specification",
            ));
        };
        let Some(kind) = fields.iter().find_map(|(key, _)| {
            key.as_str().and_then(|field| match field {
                "tar" => Some(LayerSpecType::Tar),
                "glob" => Some(LayerSpecType::Glob),
                "paths" => Some(LayerSpecType::Paths),
                "stubs" => Some(LayerSpecType::Stubs),
                "symlinks" => Some(LayerSpecType::Symlinks),
                "shared_library_dependencies" => Some(LayerSpecType::SharedLibraryDependencies),
                "command" => Some(LayerSpecType::Command),
                _ => None,
            })
        }) else {
            return Err(D::Error::custom("couldn't determine layer type"));
        };
        let deserializer = ContentRefDeserializer::<D::Error>::new(&content);
        match kind {
            LayerSpecType::Tar => TarLayerSpec::deserialize(deserializer).map(Self::Tar),
            LayerSpecType::Glob => GlobLayerSpec::deserialize(deserializer).map(Self::Glob),
            LayerSpecType::Paths => PathsLayerSpec::deserialize(deserializer).map(Self::Paths),
            LayerSpecType::Stubs => StubsLayerSpec::deserialize(deserializer).map(Self::Stubs),
            LayerSpecType::Symlinks => {
                SymlinksLayerSpec::deserialize(deserializer).map(Self::Symlinks)
            }
            LayerSpecType::SharedLibraryDependencies => {
                SharedLibraryDependenciesLayerSpec::deserialize(deserializer)
                    .map(Self::SharedLibraryDependencies)
            }
            LayerSpecType::Command => {
                CommandLayerSpec::deserialize(deserializer).map(Self::Command)
            }
        }
    }
}

impl LayerSpec {
    pub fn replace_template_variables(&mut self, vars: &TemplateVariables) -> Result<()> {
        match self {
            Self::Tar(TarLayerSpec { path }) => *path = vars.replace(path)?.into(),
            Self::Glob(GlobLayerSpec { glob, .. }) => *glob = vars.replace(glob)?,
            Self::Paths(PathsLayerSpec { paths, .. }) => {
                for path in paths {
                    *path = vars.replace(path)?.into();
                }
            }
            Self::Stubs(StubsLayerSpec { stubs, .. }) => {
                for stub in stubs {
                    *stub = vars.replace(stub)?;
                }
            }
            Self::Symlinks(SymlinksLayerSpec { symlinks }) => {
                for SymlinkSpec { link, target } in symlinks {
                    *link = vars.replace(link)?.into();
                    *target = vars.replace(target)?.into();
                }
            }
            Self::SharedLibraryDependencies(SharedLibraryDependenciesLayerSpec {
                binary_paths,
                ..
            }) => {
                for path in binary_paths {
                    *path = vars.replace(path)?.into();
                }
            }
            Self::Command(_) => {}
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
    EnableWritableFileSystem,
    Environment,
    WorkingDirectory,
    Mounts,
    Network,
    User,
    Group,
}

pub fn project_container_use_set_to_image_use_set(
    container_use: EnumSet<ContainerUse>,
) -> EnumSet<ImageUse> {
    container_use
        .into_iter()
        .filter_map(|container_use| match container_use {
            ContainerUse::Layers => Some(ImageUse::Layers),
            ContainerUse::EnableWritableFileSystem => None,
            ContainerUse::Environment => Some(ImageUse::Environment),
            ContainerUse::WorkingDirectory => Some(ImageUse::WorkingDirectory),
            ContainerUse::Mounts => None,
            ContainerUse::Network => None,
            ContainerUse::User => None,
            ContainerUse::Group => None,
        })
        .collect()
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

/// A convenience struct for extracting parts of an OCI image for use in a
/// [`maelstrom_base::JobSpec`].
#[derive(Clone, Debug)]
pub struct ConvertedImage {
    name: String,
    layers: Vec<PathBuf>,
    environment: Option<Vec<String>>,
    working_directory: Option<Utf8PathBuf>,
}

#[macro_export]
macro_rules! converted_image {
    (@expand [$name:expr] [] -> [$($($field_out:tt)+)?]) => {
        $crate::spec::ConvertedImage::new($name.into(), $crate::spec::ImageConfig {
            $($($field_out)+,)?
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
                Ok(LayerSpec::Tar(TarLayerSpec {
                    path: Utf8PathBuf::from_path_buf(p.to_owned()).map_err(|_| {
                        format!("image {} has a non-UTF-8 layer path {p:?}", self.name())
                    })?,
                }))
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
    use enumset::enum_set;
    use indoc::indoc;
    use maelstrom_base::{proc_mount, tmp_mount};
    use maelstrom_test::{path_buf, string, string_vec, utf8_path_buf};
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
                layers: vec![path_buf!("42"), path_buf!("43")],
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
            vec![tar_layer_spec!("42"), tar_layer_spec!("43")],
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

    #[test]
    fn image_ref_macro_empty() {
        assert_eq!(
            image_ref!("foo"),
            ImageRef {
                name: "foo".into(),
                r#use: enum_set!(),
            },
        );
    }

    #[test]
    fn image_ref_macro_empty_trailing_comma() {
        assert_eq!(
            image_ref!("foo",),
            ImageRef {
                name: "foo".into(),
                r#use: enum_set!(),
            },
        );
    }

    #[test]
    fn image_ref_macro_all() {
        assert_eq!(
            image_ref!("foo", all),
            ImageRef {
                name: "foo".into(),
                r#use: EnumSet::all(),
            },
        );
    }

    #[test]
    fn image_ref_macro_all_trailing_comma() {
        assert_eq!(
            image_ref!("foo", all,),
            ImageRef {
                name: "foo".into(),
                r#use: EnumSet::all(),
            },
        );
    }

    #[test]
    fn image_ref_macro_layers() {
        assert_eq!(
            image_ref!("foo", layers),
            ImageRef {
                name: "foo".into(),
                r#use: enum_set!(ImageUse::Layers),
            },
        );
    }

    #[test]
    fn image_ref_macro_layers_trailing_comma() {
        assert_eq!(
            image_ref!("foo", layers,),
            ImageRef {
                name: "foo".into(),
                r#use: enum_set!(ImageUse::Layers),
            },
        );
    }

    #[test]
    fn image_ref_macro_minus_layers() {
        assert_eq!(
            image_ref!("foo", -layers),
            ImageRef {
                name: "foo".into(),
                r#use: enum_set!(),
            },
        );
    }

    #[test]
    fn image_ref_macro_all_minus_layers() {
        assert_eq!(
            image_ref!("foo", all, -layers),
            ImageRef {
                name: "foo".into(),
                r#use: enum_set!(ImageUse::Environment | ImageUse::WorkingDirectory),
            },
        );
    }

    #[test]
    fn image_ref_macro_all_minus_layers_trailing_comma() {
        assert_eq!(
            image_ref!("foo", all, -layers,),
            ImageRef {
                name: "foo".into(),
                r#use: enum_set!(ImageUse::Environment | ImageUse::WorkingDirectory),
            },
        );
    }

    #[test]
    fn image_ref_macro_environment() {
        assert_eq!(
            image_ref!("foo", environment),
            ImageRef {
                name: "foo".into(),
                r#use: enum_set!(ImageUse::Environment),
            },
        );
    }

    #[test]
    fn image_ref_macro_environment_trailing_comma() {
        assert_eq!(
            image_ref!("foo", environment,),
            ImageRef {
                name: "foo".into(),
                r#use: enum_set!(ImageUse::Environment),
            },
        );
    }

    #[test]
    fn image_ref_macro_minus_environment() {
        assert_eq!(
            image_ref!("foo", -environment),
            ImageRef {
                name: "foo".into(),
                r#use: enum_set!(),
            },
        );
    }

    #[test]
    fn image_ref_macro_all_minus_environment() {
        assert_eq!(
            image_ref!("foo", all, -environment),
            ImageRef {
                name: "foo".into(),
                r#use: enum_set!(ImageUse::Layers | ImageUse::WorkingDirectory),
            },
        );
    }

    #[test]
    fn image_ref_macro_all_minus_environment_trailing_comma() {
        assert_eq!(
            image_ref!("foo", all, -environment,),
            ImageRef {
                name: "foo".into(),
                r#use: enum_set!(ImageUse::Layers | ImageUse::WorkingDirectory),
            },
        );
    }

    #[test]
    fn image_ref_macro_working_directory() {
        assert_eq!(
            image_ref!("foo", working_directory),
            ImageRef {
                name: "foo".into(),
                r#use: enum_set!(ImageUse::WorkingDirectory),
            },
        );
    }

    #[test]
    fn image_ref_macro_working_directory_trailing_comma() {
        assert_eq!(
            image_ref!("foo", working_directory),
            ImageRef {
                name: "foo".into(),
                r#use: enum_set!(ImageUse::WorkingDirectory),
            },
        );
    }

    #[test]
    fn image_ref_macro_minus_working_directory() {
        assert_eq!(
            image_ref!("foo", -working_directory),
            ImageRef {
                name: "foo".into(),
                r#use: enum_set!(),
            },
        );
    }

    #[test]
    fn image_ref_macro_all_minus_working_directory() {
        assert_eq!(
            image_ref!("foo", all, -working_directory),
            ImageRef {
                name: "foo".into(),
                r#use: enum_set!(ImageUse::Layers | ImageUse::Environment),
            },
        );
    }

    #[test]
    fn image_ref_macro_all_minus_working_directory_trailing_comma() {
        assert_eq!(
            image_ref!("foo", all, -working_directory,),
            ImageRef {
                name: "foo".into(),
                r#use: enum_set!(ImageUse::Layers | ImageUse::Environment),
            },
        );
    }

    #[test]
    fn image_ref_macro_union() {
        assert_eq!(
            image_ref!("foo", layers, working_directory, layers, layers),
            ImageRef {
                name: "foo".into(),
                r#use: enum_set!(ImageUse::Layers | ImageUse::WorkingDirectory),
            },
        );
    }

    #[test]
    fn image_ref_macro_union_after_all() {
        assert_eq!(
            image_ref!("foo", all, working_directory, layers, layers),
            ImageRef {
                name: "foo".into(),
                r#use: EnumSet::all(),
            },
        );
    }

    #[test]
    fn image_ref_macro_union_before_all() {
        assert_eq!(
            image_ref!("foo", working_directory, layers, layers, all),
            ImageRef {
                name: "foo".into(),
                r#use: EnumSet::all(),
            },
        );
    }

    #[test]
    fn container_ref_macro_empty() {
        assert_eq!(
            container_ref!("foo"),
            ContainerRef {
                name: "foo".into(),
                r#use: enum_set!(),
            },
        );
    }

    #[test]
    fn container_ref_macro_empty_trailing_comma() {
        assert_eq!(
            container_ref!("foo",),
            ContainerRef {
                name: "foo".into(),
                r#use: enum_set!(),
            },
        );
    }

    #[test]
    fn container_ref_macro_all() {
        assert_eq!(
            container_ref!("foo", all),
            ContainerRef {
                name: "foo".into(),
                r#use: EnumSet::all(),
            },
        );
    }

    #[test]
    fn container_ref_macro_all_trailing_comma() {
        assert_eq!(
            container_ref!("foo", all,),
            ContainerRef {
                name: "foo".into(),
                r#use: EnumSet::all(),
            },
        );
    }

    #[test]
    fn container_ref_macro_layers() {
        assert_eq!(
            container_ref!("foo", layers),
            ContainerRef {
                name: "foo".into(),
                r#use: enum_set!(ContainerUse::Layers),
            },
        );
    }

    #[test]
    fn container_ref_macro_layers_trailing_comma() {
        assert_eq!(
            container_ref!("foo", layers,),
            ContainerRef {
                name: "foo".into(),
                r#use: enum_set!(ContainerUse::Layers),
            },
        );
    }

    #[test]
    fn container_ref_macro_minus_layers() {
        assert_eq!(
            container_ref!("foo", -layers),
            ContainerRef {
                name: "foo".into(),
                r#use: enum_set!(),
            },
        );
    }

    #[test]
    fn container_ref_macro_all_minus_layers() {
        assert_eq!(
            container_ref!("foo", all, -layers),
            ContainerRef {
                name: "foo".into(),
                r#use: enum_set! {
                    ContainerUse::EnableWritableFileSystem |
                    ContainerUse::Environment |
                    ContainerUse::WorkingDirectory |
                    ContainerUse::Mounts |
                    ContainerUse::Network |
                    ContainerUse::User |
                    ContainerUse::Group
                },
            },
        );
    }

    #[test]
    fn container_ref_macro_all_minus_layers_trailing_comma() {
        assert_eq!(
            container_ref!("foo", all, -layers,),
            ContainerRef {
                name: "foo".into(),
                r#use: enum_set! {
                    ContainerUse::EnableWritableFileSystem |
                    ContainerUse::Environment |
                    ContainerUse::WorkingDirectory |
                    ContainerUse::Mounts |
                    ContainerUse::Network |
                    ContainerUse::User |
                    ContainerUse::Group
                },
            },
        );
    }

    #[test]
    fn container_ref_macro_enable_writable_file_system() {
        assert_eq!(
            container_ref!("foo", enable_writable_file_system),
            ContainerRef {
                name: "foo".into(),
                r#use: enum_set!(ContainerUse::EnableWritableFileSystem),
            },
        );
    }

    #[test]
    fn container_ref_macro_enable_writable_file_system_trailing_comma() {
        assert_eq!(
            container_ref!("foo", enable_writable_file_system,),
            ContainerRef {
                name: "foo".into(),
                r#use: enum_set!(ContainerUse::EnableWritableFileSystem),
            },
        );
    }

    #[test]
    fn container_ref_macro_minus_enable_writable_file_system() {
        assert_eq!(
            container_ref!("foo", -enable_writable_file_system),
            ContainerRef {
                name: "foo".into(),
                r#use: enum_set!(),
            },
        );
    }

    #[test]
    fn container_ref_macro_all_minus_enable_writable_file_system() {
        assert_eq!(
            container_ref!("foo", all, -enable_writable_file_system),
            ContainerRef {
                name: "foo".into(),
                r#use: enum_set! {
                    ContainerUse::Layers |
                    ContainerUse::Environment |
                    ContainerUse::WorkingDirectory |
                    ContainerUse::Mounts |
                    ContainerUse::Network |
                    ContainerUse::User |
                    ContainerUse::Group
                },
            },
        );
    }

    #[test]
    fn container_ref_macro_all_minus_enable_writable_file_system_trailing_comma() {
        assert_eq!(
            container_ref!("foo", all, -enable_writable_file_system,),
            ContainerRef {
                name: "foo".into(),
                r#use: enum_set! {
                    ContainerUse::Layers |
                    ContainerUse::Environment |
                    ContainerUse::WorkingDirectory |
                    ContainerUse::Mounts |
                    ContainerUse::Network |
                    ContainerUse::User |
                    ContainerUse::Group
                },
            },
        );
    }

    #[test]
    fn container_ref_macro_environment() {
        assert_eq!(
            container_ref!("foo", environment),
            ContainerRef {
                name: "foo".into(),
                r#use: enum_set!(ContainerUse::Environment),
            },
        );
    }

    #[test]
    fn container_ref_macro_environment_trailing_comma() {
        assert_eq!(
            container_ref!("foo", environment,),
            ContainerRef {
                name: "foo".into(),
                r#use: enum_set!(ContainerUse::Environment),
            },
        );
    }

    #[test]
    fn container_ref_macro_minus_environment() {
        assert_eq!(
            container_ref!("foo", -environment),
            ContainerRef {
                name: "foo".into(),
                r#use: enum_set!(),
            },
        );
    }

    #[test]
    fn container_ref_macro_all_minus_environment() {
        assert_eq!(
            container_ref!("foo", all, -environment),
            ContainerRef {
                name: "foo".into(),
                r#use: enum_set! {
                    ContainerUse::Layers |
                    ContainerUse::EnableWritableFileSystem |
                    ContainerUse::WorkingDirectory |
                    ContainerUse::Mounts |
                    ContainerUse::Network |
                    ContainerUse::User |
                    ContainerUse::Group
                },
            },
        );
    }

    #[test]
    fn container_ref_macro_all_minus_environment_trailing_comma() {
        assert_eq!(
            container_ref!("foo", all, -environment,),
            ContainerRef {
                name: "foo".into(),
                r#use: enum_set! {
                    ContainerUse::Layers |
                    ContainerUse::EnableWritableFileSystem |
                    ContainerUse::WorkingDirectory |
                    ContainerUse::Mounts |
                    ContainerUse::Network |
                    ContainerUse::User |
                    ContainerUse::Group
                },
            },
        );
    }

    #[test]
    fn container_ref_macro_working_directory() {
        assert_eq!(
            container_ref!("foo", working_directory),
            ContainerRef {
                name: "foo".into(),
                r#use: enum_set!(ContainerUse::WorkingDirectory),
            },
        );
    }

    #[test]
    fn container_ref_macro_working_directory_trailing_comma() {
        assert_eq!(
            container_ref!("foo", working_directory),
            ContainerRef {
                name: "foo".into(),
                r#use: enum_set!(ContainerUse::WorkingDirectory),
            },
        );
    }

    #[test]
    fn container_ref_macro_minus_working_directory() {
        assert_eq!(
            container_ref!("foo", -working_directory),
            ContainerRef {
                name: "foo".into(),
                r#use: enum_set!(),
            },
        );
    }

    #[test]
    fn container_ref_macro_all_minus_working_directory() {
        assert_eq!(
            container_ref!("foo", all, -working_directory),
            ContainerRef {
                name: "foo".into(),
                r#use: enum_set! {
                    ContainerUse::Layers |
                    ContainerUse::EnableWritableFileSystem |
                    ContainerUse::Environment |
                    ContainerUse::Mounts |
                    ContainerUse::Network |
                    ContainerUse::User |
                    ContainerUse::Group
                },
            },
        );
    }

    #[test]
    fn container_ref_macro_all_minus_working_directory_trailing_comma() {
        assert_eq!(
            container_ref!("foo", all, -working_directory,),
            ContainerRef {
                name: "foo".into(),
                r#use: enum_set! {
                    ContainerUse::Layers |
                    ContainerUse::EnableWritableFileSystem |
                    ContainerUse::Environment |
                    ContainerUse::Mounts |
                    ContainerUse::Network |
                    ContainerUse::User |
                    ContainerUse::Group
                },
            },
        );
    }

    #[test]
    fn container_ref_macro_mounts() {
        assert_eq!(
            container_ref!("foo", mounts),
            ContainerRef {
                name: "foo".into(),
                r#use: enum_set!(ContainerUse::Mounts),
            },
        );
    }

    #[test]
    fn container_ref_macro_mounts_trailing_comma() {
        assert_eq!(
            container_ref!("foo", mounts),
            ContainerRef {
                name: "foo".into(),
                r#use: enum_set!(ContainerUse::Mounts),
            },
        );
    }

    #[test]
    fn container_ref_macro_minus_mounts() {
        assert_eq!(
            container_ref!("foo", -mounts),
            ContainerRef {
                name: "foo".into(),
                r#use: enum_set!(),
            },
        );
    }

    #[test]
    fn container_ref_macro_all_minus_mounts() {
        assert_eq!(
            container_ref!("foo", all, -mounts),
            ContainerRef {
                name: "foo".into(),
                r#use: enum_set! {
                    ContainerUse::Layers |
                    ContainerUse::EnableWritableFileSystem |
                    ContainerUse::Environment |
                    ContainerUse::WorkingDirectory |
                    ContainerUse::Network |
                    ContainerUse::User |
                    ContainerUse::Group
                },
            },
        );
    }

    #[test]
    fn container_ref_macro_all_minus_mounts_trailing_comma() {
        assert_eq!(
            container_ref!("foo", all, -mounts,),
            ContainerRef {
                name: "foo".into(),
                r#use: enum_set! {
                    ContainerUse::Layers |
                    ContainerUse::EnableWritableFileSystem |
                    ContainerUse::Environment |
                    ContainerUse::WorkingDirectory |
                    ContainerUse::Network |
                    ContainerUse::User |
                    ContainerUse::Group
                },
            },
        );
    }

    #[test]
    fn container_ref_macro_network() {
        assert_eq!(
            container_ref!("foo", network),
            ContainerRef {
                name: "foo".into(),
                r#use: enum_set!(ContainerUse::Network),
            },
        );
    }

    #[test]
    fn container_ref_macro_network_trailing_comma() {
        assert_eq!(
            container_ref!("foo", network),
            ContainerRef {
                name: "foo".into(),
                r#use: enum_set!(ContainerUse::Network),
            },
        );
    }

    #[test]
    fn container_ref_macro_minus_network() {
        assert_eq!(
            container_ref!("foo", -network),
            ContainerRef {
                name: "foo".into(),
                r#use: enum_set!(),
            },
        );
    }

    #[test]
    fn container_ref_macro_all_minus_network() {
        assert_eq!(
            container_ref!("foo", all, -network),
            ContainerRef {
                name: "foo".into(),
                r#use: enum_set! {
                    ContainerUse::Layers |
                    ContainerUse::EnableWritableFileSystem |
                    ContainerUse::Environment |
                    ContainerUse::WorkingDirectory |
                    ContainerUse::Mounts |
                    ContainerUse::User |
                    ContainerUse::Group
                },
            },
        );
    }

    #[test]
    fn container_ref_macro_all_minus_network_trailing_comma() {
        assert_eq!(
            container_ref!("foo", all, -network,),
            ContainerRef {
                name: "foo".into(),
                r#use: enum_set! {
                    ContainerUse::Layers |
                    ContainerUse::EnableWritableFileSystem |
                    ContainerUse::Environment |
                    ContainerUse::WorkingDirectory |
                    ContainerUse::Mounts |
                    ContainerUse::User |
                    ContainerUse::Group
                },
            },
        );
    }

    #[test]
    fn container_ref_macro_user() {
        assert_eq!(
            container_ref!("foo", user),
            ContainerRef {
                name: "foo".into(),
                r#use: enum_set!(ContainerUse::User),
            },
        );
    }

    #[test]
    fn container_ref_macro_user_trailing_comma() {
        assert_eq!(
            container_ref!("foo", user),
            ContainerRef {
                name: "foo".into(),
                r#use: enum_set!(ContainerUse::User),
            },
        );
    }

    #[test]
    fn container_ref_macro_minus_user() {
        assert_eq!(
            container_ref!("foo", -user),
            ContainerRef {
                name: "foo".into(),
                r#use: enum_set!(),
            },
        );
    }

    #[test]
    fn container_ref_macro_all_minus_user() {
        assert_eq!(
            container_ref!("foo", all, -user),
            ContainerRef {
                name: "foo".into(),
                r#use: enum_set! {
                    ContainerUse::Layers |
                    ContainerUse::EnableWritableFileSystem |
                    ContainerUse::Environment |
                    ContainerUse::WorkingDirectory |
                    ContainerUse::Mounts |
                    ContainerUse::Network |
                    ContainerUse::Group
                },
            },
        );
    }

    #[test]
    fn container_ref_macro_all_minus_user_trailing_comma() {
        assert_eq!(
            container_ref!("foo", all, -user,),
            ContainerRef {
                name: "foo".into(),
                r#use: enum_set! {
                    ContainerUse::Layers |
                    ContainerUse::EnableWritableFileSystem |
                    ContainerUse::Environment |
                    ContainerUse::WorkingDirectory |
                    ContainerUse::Mounts |
                    ContainerUse::Network |
                    ContainerUse::Group
                },
            },
        );
    }

    #[test]
    fn container_ref_macro_group() {
        assert_eq!(
            container_ref!("foo", group),
            ContainerRef {
                name: "foo".into(),
                r#use: enum_set!(ContainerUse::Group),
            },
        );
    }

    #[test]
    fn container_ref_macro_group_trailing_comma() {
        assert_eq!(
            container_ref!("foo", group),
            ContainerRef {
                name: "foo".into(),
                r#use: enum_set!(ContainerUse::Group),
            },
        );
    }

    #[test]
    fn container_ref_macro_minus_group() {
        assert_eq!(
            container_ref!("foo", -group),
            ContainerRef {
                name: "foo".into(),
                r#use: enum_set!(),
            },
        );
    }

    #[test]
    fn container_ref_macro_all_minus_group() {
        assert_eq!(
            container_ref!("foo", all, -group),
            ContainerRef {
                name: "foo".into(),
                r#use: enum_set! {
                    ContainerUse::Layers |
                    ContainerUse::EnableWritableFileSystem |
                    ContainerUse::Environment |
                    ContainerUse::WorkingDirectory |
                    ContainerUse::Mounts |
                    ContainerUse::Network |
                    ContainerUse::User
                },
            },
        );
    }

    #[test]
    fn container_ref_macro_all_minus_group_trailing_comma() {
        assert_eq!(
            container_ref!("foo", all, -group,),
            ContainerRef {
                name: "foo".into(),
                r#use: enum_set! {
                    ContainerUse::Layers |
                    ContainerUse::EnableWritableFileSystem |
                    ContainerUse::Environment |
                    ContainerUse::WorkingDirectory |
                    ContainerUse::Mounts |
                    ContainerUse::Network |
                    ContainerUse::User
                },
            },
        );
    }

    #[test]
    fn container_ref_macro_union() {
        assert_eq!(
            container_ref!("foo", layers, working_directory, layers, layers),
            ContainerRef {
                name: "foo".into(),
                r#use: enum_set!(ContainerUse::Layers | ContainerUse::WorkingDirectory),
            },
        );
    }

    #[test]
    fn container_ref_macro_union_after_all() {
        assert_eq!(
            container_ref!("foo", all, working_directory, layers, layers),
            ContainerRef {
                name: "foo".into(),
                r#use: EnumSet::all(),
            },
        );
    }

    #[test]
    fn container_ref_macro_union_before_all() {
        assert_eq!(
            container_ref!("foo", working_directory, layers, layers, all),
            ContainerRef {
                name: "foo".into(),
                r#use: EnumSet::all(),
            },
        );
    }

    #[derive(Debug, Deserialize, PartialEq)]
    struct ImageRefContainer {
        image: ImageRefWithImplicitOrExplicitUse,
    }

    impl ImageRefContainer {
        fn new(image: ImageRefWithImplicitOrExplicitUse) -> Self {
            Self { image }
        }
    }

    fn parse_image_container(file: &str) -> ImageRefContainer {
        toml::from_str(file).unwrap()
    }

    #[test]
    fn image_ref_deserialize_explicit_use_of_all() {
        assert_eq!(
            parse_image_container(indoc! {r#"
                [image]
                name = "name"
                use = [ "layers", "environment", "working_directory" ]
            "#}),
            ImageRefContainer::new(ImageRefWithImplicitOrExplicitUse {
                name: "name".into(),
                r#use: ImplicitOrExplicitUse::Explicit(EnumSet::all()),
            }),
        );
    }

    #[test]
    fn image_ref_deserialize_explicit_use_of_one() {
        assert_eq!(
            parse_image_container(indoc! {r#"
                [image]
                name = "name"
                use = [ "environment" ]
            "#}),
            ImageRefContainer::new(ImageRefWithImplicitOrExplicitUse {
                name: "name".into(),
                r#use: ImplicitOrExplicitUse::Explicit(ImageUse::Environment.into()),
            }),
        );
    }

    #[test]
    fn image_ref_deserialize_implicit_only_name() {
        assert_eq!(
            parse_image_container(indoc! {r#"
                [image]
                name = "name"
            "#}),
            ImageRefContainer::new(ImageRefWithImplicitOrExplicitUse {
                name: "name".into(),
                r#use: ImplicitOrExplicitUse::Implicit,
            }),
        );
    }

    #[test]
    fn image_ref_deserialize_implicit_as_string() {
        assert_eq!(
            parse_image_container(indoc! {r#"
                image = "name"
            "#}),
            ImageRefContainer::new(ImageRefWithImplicitOrExplicitUse {
                name: "name".into(),
                r#use: ImplicitOrExplicitUse::Implicit,
            }),
        );
    }

    #[test]
    fn implicit_or_explicit_image_use_implicit() {
        let r#use = ImplicitOrExplicitUse::Implicit;
        assert_eq!(r#use.explicit(), enum_set!());
        assert_eq!(
            r#use.as_set(),
            enum_set!(ImageUse::Layers | ImageUse::Environment | ImageUse::WorkingDirectory),
        );
    }

    #[test]
    fn implicit_or_explicit_image_use_explicit() {
        let r#use = ImplicitOrExplicitUse::Explicit(enum_set!(
            ImageUse::Layers | ImageUse::WorkingDirectory
        ));
        assert_eq!(
            r#use.explicit(),
            enum_set!(ImageUse::Layers | ImageUse::WorkingDirectory),
        );
        assert_eq!(
            r#use.as_set(),
            enum_set!(ImageUse::Layers | ImageUse::WorkingDirectory),
        );
    }

    mod environment_spec {
        use super::*;

        #[test]
        fn empty() {
            assert_eq!(
                environment_spec!(),
                EnvironmentSpec {
                    vars: Default::default(),
                    extend: true,
                },
            );
        }

        #[test]
        fn only_extend() {
            assert_eq!(
                environment_spec!(extend: true),
                EnvironmentSpec {
                    vars: Default::default(),
                    extend: true,
                },
            );
            assert_eq!(
                environment_spec!(extend: false),
                EnvironmentSpec {
                    vars: Default::default(),
                    extend: false,
                },
            );
            assert_eq!(
                environment_spec!(extend: true,),
                EnvironmentSpec {
                    vars: Default::default(),
                    extend: true,
                },
            );
            assert_eq!(
                environment_spec!(extend: false,),
                EnvironmentSpec {
                    vars: Default::default(),
                    extend: false,
                },
            );
        }

        #[test]
        fn one_pair() {
            assert_eq!(
                environment_spec!("foo" => "bar"),
                EnvironmentSpec {
                    vars: btreemap! { "foo".into() => "bar".into() },
                    extend: true,
                },
            );
            assert_eq!(
                environment_spec!("foo" => "bar",),
                EnvironmentSpec {
                    vars: btreemap! { "foo".into() => "bar".into() },
                    extend: true,
                },
            );
        }

        #[test]
        fn one_pair_with_extend_before() {
            assert_eq!(
                environment_spec!(extend: false, "foo" => "bar"),
                EnvironmentSpec {
                    vars: btreemap! { "foo".into() => "bar".into() },
                    extend: false,
                },
            );
            assert_eq!(
                environment_spec!(extend: false, "foo" => "bar",),
                EnvironmentSpec {
                    vars: btreemap! { "foo".into() => "bar".into() },
                    extend: false,
                },
            );
        }

        #[test]
        fn one_pair_with_extend_after() {
            assert_eq!(
                environment_spec!("foo" => "bar", extend: false),
                EnvironmentSpec {
                    vars: btreemap! { "foo".into() => "bar".into() },
                    extend: false,
                },
            );
            assert_eq!(
                environment_spec!("foo" => "bar", extend: false,),
                EnvironmentSpec {
                    vars: btreemap! { "foo".into() => "bar".into() },
                    extend: false,
                },
            );
        }

        #[test]
        fn two_pairs_with_extend_before() {
            assert_eq!(
                environment_spec!(extend: false, "foo" => "bar", "frob" => "baz"),
                EnvironmentSpec {
                    vars: btreemap! {
                        "foo".into() => "bar".into(),
                        "frob".into() => "baz".into(),
                    },
                    extend: false,
                },
            );
            assert_eq!(
                environment_spec!(extend: false, "foo" => "bar", "frob" => "baz",),
                EnvironmentSpec {
                    vars: btreemap! {
                        "foo".into() => "bar".into(),
                        "frob".into() => "baz".into(),
                    },
                    extend: false,
                },
            );
        }

        #[test]
        fn two_pairs_with_extend_in_between() {
            assert_eq!(
                environment_spec!("foo" => "bar", extend: false, "frob" => "baz"),
                EnvironmentSpec {
                    vars: btreemap! {
                        "foo".into() => "bar".into(),
                        "frob".into() => "baz".into(),
                    },
                    extend: false,
                },
            );
            assert_eq!(
                environment_spec!("foo" => "bar", extend: false, "frob" => "baz",),
                EnvironmentSpec {
                    vars: btreemap! {
                        "foo".into() => "bar".into(),
                        "frob".into() => "baz".into(),
                    },
                    extend: false,
                },
            );
        }

        #[test]
        fn two_pairs_with_extend_after() {
            assert_eq!(
                environment_spec!("foo" => "bar", "frob" => "baz", extend: false),
                EnvironmentSpec {
                    vars: btreemap! {
                        "foo".into() => "bar".into(),
                        "frob".into() => "baz".into(),
                    },
                    extend: false,
                },
            );
            assert_eq!(
                environment_spec!("foo" => "bar", "frob" => "baz", extend: false,),
                EnvironmentSpec {
                    vars: btreemap! {
                        "foo".into() => "bar".into(),
                        "frob".into() => "baz".into(),
                    },
                    extend: false,
                },
            );
        }
    }

    #[track_caller]
    fn parse_toml<T: for<'de> Deserialize<'de>>(file: &str) -> T {
        toml::from_str(file).unwrap()
    }

    #[track_caller]
    fn parse_error_toml<T: std::fmt::Debug + for<'de> Deserialize<'de>>(file: &str) -> String {
        format!("{}", toml::from_str::<T>(file).unwrap_err())
            .trim_end()
            .into()
    }

    #[track_caller]
    fn parse_json<T: for<'de> Deserialize<'de>>(file: &str) -> T {
        serde_json::from_str(file).unwrap()
    }

    #[track_caller]
    fn parse_error_json<T: std::fmt::Debug + for<'de> Deserialize<'de>>(file: &str) -> String {
        format!("{}", serde_json::from_str::<T>(file).unwrap_err())
    }

    mod symlink_spec {
        use super::*;

        #[test]
        fn empty() {
            assert!(parse_error_json::<SymlinkSpec>("{}").contains("missing field `link`"));
        }

        #[test]
        fn unknown_field() {
            assert!(parse_error_toml::<SymlinkSpec>(indoc! {r#"
                link = "/symlink"
                target = "/target"
                foo_bar_baz = 3
            "#})
            .contains("unknown field `foo_bar_baz`"));
        }

        #[test]
        fn symlink_spec() {
            assert_eq!(
                parse_toml::<SymlinkSpec>(indoc! {r#"
                    link = "/symlink"
                    target = "/target"
                "#}),
                symlink_spec!("/symlink" => "/target")
            );
        }

        #[test]
        fn missing_link() {
            assert!(parse_error_toml::<SymlinkSpec>(indoc! {r#"
                target = "/target"
            "#})
            .contains("missing field `link`"));
        }

        #[test]
        fn missing_target() {
            assert!(parse_error_toml::<SymlinkSpec>(indoc! {r#"
                link = "/symlink"
            "#})
            .contains("missing field `target`"));
        }
    }

    mod layer_spec {
        use super::*;

        #[test]
        fn empty() {
            assert!(parse_error_json::<LayerSpec>("{}").contains("couldn't determine layer type"));
        }

        #[test]
        fn unknown_field() {
            assert!(parse_error_toml::<LayerSpec>(indoc! {r#"
                foo_bar_baz = 3
            "#})
            .contains("couldn't determine layer type"));
        }

        #[test]
        fn not_map() {
            assert!(parse_error_json::<LayerSpec>("42")
                .contains("expected a map in order to parse a layer specification"));
        }

        mod tar {
            use super::*;

            #[test]
            fn path() {
                assert_eq!(
                    parse_toml::<LayerSpec>(indoc! {r#"
                        tar = "foo.tar"
                    "#}),
                    tar_layer_spec!("foo.tar"),
                );
            }

            #[test]
            fn prefix_options_field() {
                assert!(parse_error_json::<LayerSpec>(indoc! {r#"{
                    "tar": "foo.tar",
                    "strip_prefix": "/foo"
                }"#})
                .contains("unknown field `strip_prefix`"));
            }

            #[test]
            fn unknown_field() {
                assert!(parse_error_toml::<LayerSpec>(indoc! {r#"
                    tar = "foo.tar"
                    foo_bar_baz = 3
                "#})
                .contains("unknown field `foo_bar_baz`"));
            }
        }

        mod glob {
            use super::*;

            #[test]
            fn glob() {
                assert_eq!(
                    parse_toml::<LayerSpec>(indoc! {r#"
                        glob = "*.tar"
                    "#}),
                    glob_layer_spec!("*.tar"),
                );
            }

            #[test]
            fn strip_prefix() {
                assert_eq!(
                    parse_json::<LayerSpec>(indoc! {r#"{
                        "glob": "*.tar",
                        "strip_prefix": "/foo"
                    }"#}),
                    glob_layer_spec! {
                        "*.tar",
                        strip_prefix: "/foo",
                    },
                );
            }

            #[test]
            fn prepend_prefix() {
                assert_eq!(
                    parse_toml::<LayerSpec>(indoc! {r#"
                        glob = "*.tar"
                        prepend_prefix = "/bar"
                    "#}),
                    glob_layer_spec! {
                        "*.tar",
                        prepend_prefix: "/bar",
                    },
                );
            }

            #[test]
            fn canonicalize() {
                assert_eq!(
                    parse_json::<LayerSpec>(indoc! {r#"{
                        "glob": "*.tar",
                        "canonicalize": true
                    }"#}),
                    glob_layer_spec! {
                        "*.tar",
                        canonicalize: true,
                    },
                );
            }

            #[test]
            fn follow_symlinks() {
                assert_eq!(
                    parse_toml::<LayerSpec>(indoc! {r#"
                        glob = "*.tar"
                        follow_symlinks = true
                    "#}),
                    glob_layer_spec! {
                        "*.tar",
                        follow_symlinks: true,
                    },
                );
            }

            #[test]
            fn unknown_field() {
                assert!(parse_error_toml::<LayerSpec>(indoc! {r#"
                    glob = "*.tar"
                    foo_bar_baz = 3
                "#})
                .contains("unknown field `foo_bar_baz`"));
            }
        }

        mod paths {
            use super::*;

            #[test]
            fn paths() {
                assert_eq!(
                    parse_toml::<LayerSpec>(indoc! {r#"
                        paths = [ "/foo/bar", "/bar/foo" ]
                    "#}),
                    paths_layer_spec!(["/foo/bar", "/bar/foo"]),
                );
            }

            #[test]
            fn strip_prefix() {
                assert_eq!(
                    parse_json::<LayerSpec>(indoc! {r#"{
                        "paths": [ "/foo/bar", "/bar/foo" ],
                        "strip_prefix": "/foo"
                    }"#}),
                    paths_layer_spec! {
                        ["/foo/bar", "/bar/foo"],
                        strip_prefix: "/foo",
                    },
                );
            }

            #[test]
            fn prepend_prefix() {
                assert_eq!(
                    parse_toml::<LayerSpec>(indoc! {r#"
                        paths = [ "/foo/bar", "/bar/foo" ]
                        prepend_prefix = "/bar"
                    "#}),
                    paths_layer_spec! {
                        ["/foo/bar", "/bar/foo"],
                        prepend_prefix: "/bar",
                    },
                );
            }

            #[test]
            fn canonicalize() {
                assert_eq!(
                    parse_json::<LayerSpec>(indoc! {r#"{
                        "paths": [ "/foo/bar", "/bar/foo" ],
                        "canonicalize": true
                    }"#}),
                    paths_layer_spec! {
                        ["/foo/bar", "/bar/foo"],
                        canonicalize: true,
                    },
                );
            }

            #[test]
            fn follow_symlinks() {
                assert_eq!(
                    parse_toml::<LayerSpec>(indoc! {r#"
                        paths = [ "/foo/bar", "/bar/foo" ]
                        follow_symlinks = true
                    "#}),
                    paths_layer_spec! {
                        ["/foo/bar", "/bar/foo"],
                        follow_symlinks: true,
                    },
                );
            }

            #[test]
            fn unknown_field() {
                assert!(parse_error_toml::<LayerSpec>(indoc! {r#"
                    paths = [ "/foo/bar", "/bar/foo" ]
                    foo_bar_baz = 3
                "#})
                .contains("unknown field `foo_bar_baz`"));
            }
        }

        mod stubs {
            use super::*;

            #[test]
            fn stubs() {
                assert_eq!(
                    parse_toml::<LayerSpec>(indoc! {r#"
                        stubs = [ "/{proc,tmp}/", "/home" ]
                    "#}),
                    stubs_layer_spec!(["/{proc,tmp}/", "/home"]),
                );
            }

            #[test]
            fn prefix_options_field() {
                assert!(parse_error_json::<LayerSpec>(indoc! {r#"{
                    "stubs": [ "/{proc,tmp}/", "/home" ],
                    "strip_prefix": "/foo"
                }"#})
                .contains("unknown field `strip_prefix`"));
            }

            #[test]
            fn unknown_field() {
                assert!(parse_error_toml::<LayerSpec>(indoc! {r#"
                    stubs = [ "/{proc,tmp}/", "/home" ]
                    foo_bar_baz = 3
                "#})
                .contains("unknown field `foo_bar_baz`"));
            }
        }

        mod symlinks {
            use super::*;

            #[test]
            fn symlinks() {
                assert_eq!(
                    parse_toml::<LayerSpec>(indoc! {r#"
                        symlinks = [
                            { link = "/symlink", target = "/target" },
                            { link = "/symlink2", target = "/target2" },
                        ]
                    "#}),
                    symlinks_layer_spec!([
                        symlink_spec!("/symlink" => "/target"),
                        symlink_spec!("/symlink2" => "/target2"),
                    ]),
                );
            }

            #[test]
            fn prefix_options_field() {
                assert!(parse_error_json::<LayerSpec>(indoc! {r#"{
                    "symlinks": [ { "link": "/symlink", "target": "/target" } ],
                    "strip_prefix": "/foo"
                }"#})
                .contains("unknown field `strip_prefix`"));
            }

            #[test]
            fn unknown_field() {
                assert!(parse_error_toml::<LayerSpec>(indoc! {r#"
                    symlinks = [
                        { link = "/symlink", target = "/target" },
                        { link = "/symlink2", target = "/target2" },
                    ]
                    foo_bar_baz = 3
                "#})
                .contains("unknown field `foo_bar_baz`"));
            }
        }

        mod shared_library_dependencies {
            use super::*;

            #[test]
            fn shared_library_dependencies() {
                assert_eq!(
                    parse_toml::<LayerSpec>(indoc! {r#"
                        shared_library_dependencies = ["/bin/foo", "/bin/bar"]
                    "#}),
                    shared_library_dependencies_layer_spec!(["/bin/foo", "/bin/bar"]),
                );
            }

            #[test]
            fn strip_prefix() {
                assert_eq!(
                    parse_json::<LayerSpec>(indoc! {r#"{
                        "shared_library_dependencies": ["/bin/foo", "/bin/bar"],
                        "strip_prefix": "/foo"
                    }"#}),
                    shared_library_dependencies_layer_spec! {
                        ["/bin/foo", "/bin/bar"],
                        strip_prefix: "/foo",
                    },
                );
            }

            #[test]
            fn prepend_prefix() {
                assert_eq!(
                    parse_toml::<LayerSpec>(indoc! {r#"
                        shared_library_dependencies = ["/bin/foo", "/bin/bar"]
                        prepend_prefix = "/bar"
                    "#}),
                    shared_library_dependencies_layer_spec! {
                        ["/bin/foo", "/bin/bar"],
                        prepend_prefix: "/bar",
                    },
                );
            }

            #[test]
            fn canonicalize() {
                assert_eq!(
                    parse_json::<LayerSpec>(indoc! {r#"{
                        "shared_library_dependencies": ["/bin/foo", "/bin/bar"],
                        "canonicalize": true
                    }"#}),
                    shared_library_dependencies_layer_spec! {
                        ["/bin/foo", "/bin/bar"],
                        canonicalize: true,
                    },
                );
            }

            #[test]
            fn follow_symlinks() {
                assert_eq!(
                    parse_toml::<LayerSpec>(indoc! {r#"
                        shared_library_dependencies = ["/bin/foo", "/bin/bar"]
                        follow_symlinks = true
                    "#}),
                    shared_library_dependencies_layer_spec! {
                        ["/bin/foo", "/bin/bar"],
                        follow_symlinks: true,
                    },
                );
            }

            #[test]
            fn unknown_field() {
                assert!(parse_error_toml::<LayerSpec>(indoc! {r#"
                    shared_library_dependencies = ["/bin/foo", "/bin/bar"]
                    foo_bar_baz = 3
                "#})
                .contains("unknown field `foo_bar_baz`"));
            }
        }

        mod command {
            use super::*;

            #[test]
            fn command_without_arguments() {
                assert_eq!(
                    parse_toml::<LayerSpec>(indoc! {r#"
                        command = "touch"
                    "#}),
                    command_layer_spec!("touch"),
                );
            }

            #[test]
            fn command_with_arguments() {
                assert_eq!(
                    parse_toml::<LayerSpec>(indoc! {r#"
                        command = "cp"
                        arguments = ["foo", "bar"]
                    "#}),
                    command_layer_spec!("cp", "foo", "bar"),
                );
            }

            #[test]
            fn unknown_field() {
                assert!(parse_error_toml::<LayerSpec>(indoc! {r#"
                    command = "touch"
                    foo_bar_baz = 3
                "#})
                .contains("unknown field `foo_bar_baz`"));
            }
        }

        mod replace_template_variables {
            use super::*;

            #[track_caller]
            fn replace_template_variables_test(toml: &str, expected: LayerSpec) {
                let mut actual: LayerSpec = parse_toml(toml);
                actual
                    .replace_template_variables(&TemplateVariables::new([
                        ("foo", "foo-value"),
                        ("bar", "bar-value"),
                    ]))
                    .unwrap();
                assert_eq!(actual, expected);
            }

            #[test]
            fn tar() {
                replace_template_variables_test(
                    indoc! {r#"
                        tar = "<foo>/<bar>/baz.tar"
                    "#},
                    tar_layer_spec!("foo-value/bar-value/baz.tar"),
                );
            }

            #[test]
            fn glob() {
                replace_template_variables_test(
                    indoc! {r#"
                        glob = "<foo>/<bar>/*.tar"
                        strip_prefix = "<foo>/"
                        prepend_prefix = "<bar>/"
                    "#},
                    glob_layer_spec! {
                        "foo-value/bar-value/*.tar",
                        strip_prefix: "<foo>/",
                        prepend_prefix: "<bar>/",
                    },
                );
            }

            #[test]
            fn paths() {
                replace_template_variables_test(
                    indoc! {r#"
                        paths = ["<foo>/foo.tar", "<bar>/bar.tar"]
                        strip_prefix = "<foo>/"
                        prepend_prefix = "<bar>/"
                    "#},
                    paths_layer_spec! {
                        ["foo-value/foo.tar", "bar-value/bar.tar"],
                        strip_prefix: "<foo>/",
                        prepend_prefix: "<bar>/",
                    },
                );
            }

            #[test]
            fn stubs() {
                replace_template_variables_test(
                    indoc! {r#"
                        stubs = ["<foo>/foo/", "<bar>/bar/"]
                    "#},
                    stubs_layer_spec!(["foo-value/foo/", "bar-value/bar/"]),
                );
            }

            #[test]
            fn symlinks() {
                replace_template_variables_test(
                    indoc! {r#"
                        symlinks = [ { link = "<foo>/symlink", target = "<bar>/target"} ]
                    "#},
                    symlinks_layer_spec!([
                        symlink_spec!("foo-value/symlink" => "bar-value/target")
                    ]),
                );
            }

            #[test]
            fn command() {
                replace_template_variables_test(
                    indoc! {r#"
                        command = "cp"
                        arguments = ["foo", "bar"]
                    "#},
                    command_layer_spec!("cp", "foo", "bar"),
                );
            }
        }
    }

    mod container_spec {
        use super::*;

        #[track_caller]
        fn parse_container_spec_toml(file: &str) -> ContainerSpec {
            toml::from_str(file).unwrap()
        }

        #[track_caller]
        fn parse_container_spec_error_toml(file: &str) -> String {
            format!("{}", toml::from_str::<ContainerSpec>(file).unwrap_err())
                .trim_end()
                .into()
        }

        #[track_caller]
        fn parse_container_spec_json(file: &str) -> ContainerSpec {
            serde_json::from_str(file).unwrap()
        }

        #[track_caller]
        fn parse_container_spec_error_json(file: &str) -> String {
            format!(
                "{}",
                serde_json::from_str::<ContainerSpec>(file).unwrap_err()
            )
        }

        #[test]
        fn image_and_parent() {
            assert_eq!(
                parse_container_spec_error_json(indoc! {r#"{
                    "image": "image",
                    "parent": "parent"
                }"#}),
                "both `image` and `parent` cannot be specified",
            );
        }

        #[test]
        fn empty() {
            assert_eq!(parse_container_spec_json("{}"), container_spec! {});
        }

        #[test]
        fn unknown_field() {
            assert!(parse_container_spec_error_toml(indoc! {r#"
                foo_bar_baz = 3
            "#})
            .contains("unknown field `foo_bar_baz`"));
        }

        mod layers {
            use super::*;

            #[test]
            fn added_layers_and_layers() {
                assert_eq!(
                    parse_container_spec_error_toml(indoc! {r#"
                        layers = [ { tar = "1" }, { tar = "2" } ]
                        added_layers = [ { tar = "3" } ]
                    "#}),
                    "field `added_layers` cannot be set with `layers` field",
                );
            }

            #[test]
            fn added_layers_and_image_with_implicit_layers() {
                assert_eq!(
                    parse_container_spec_toml(indoc! {r#"
                        image = "image1"
                        added_layers = [ { tar = "1" } ]
                    "#}),
                    container_spec! {
                        layers: [tar_layer_spec!("1")],
                        parent: image_container_parent!("image1", all),
                    },
                );
            }

            #[test]
            fn added_layers_and_image_with_explicit_layers() {
                assert_eq!(
                    parse_container_spec_json(indoc! {r#"{
                        "image": {
                            "name": "image1",
                            "use": [ "layers" ]
                        },
                        "added_layers": [ { "tar": "1" } ]
                    }"#}),
                    container_spec! {
                        layers: [tar_layer_spec!("1")],
                        parent: image_container_parent!("image1", layers),
                    },
                );
            }

            #[test]
            fn added_layers_and_image_without_layers() {
                assert_eq!(
                    parse_container_spec_error_toml(indoc! {r#"
                        image.name = "image1"
                        image.use = [ "environment" ]
                        added_layers = [ { tar = "1" } ]
                    "#}),
                    concat!(
                        "field `added_layers` requires `image` being specified with a ",
                        "`use` of `layers` (try `layers` instead)",
                    ),
                );
            }

            #[test]
            fn added_layers_and_parent_with_implicit_layers() {
                assert_eq!(
                    parse_container_spec_json(indoc! {r#"{
                        "parent": "parent",
                        "added_layers": [ { "tar": "1" } ]
                    }"#}),
                    container_spec! {
                        layers: [tar_layer_spec!("1")],
                        parent: container_container_parent!("parent", all),
                    },
                );
            }

            #[test]
            fn added_layers_and_parent_with_explicit_layers() {
                assert_eq!(
                    parse_container_spec_toml(indoc! {r#"
                        parent.name = "parent"
                        parent.use = [ "layers" ]
                        added_layers = [ { tar = "1" } ]
                    "#}),
                    container_spec! {
                        layers: [tar_layer_spec!("1")],
                        parent: container_container_parent!("parent", layers),
                    },
                );
            }

            #[test]
            fn added_layers_and_parent_without_layers() {
                assert_eq!(
                    parse_container_spec_error_json(indoc! {r#"{
                        "parent": {
                            "name": "parent",
                            "use": [ "environment" ]
                        },
                        "added_layers": [ { "tar": "1" } ]
                    }"#}),
                    concat!(
                        "field `added_layers` requires `parent` being specified with a ",
                        "`use` of `layers` (try `layers` instead)",
                    ),
                );
            }

            #[test]
            fn added_layers_and_neither_image_nor_parent() {
                assert_eq!(
                    parse_container_spec_error_toml(indoc! {r#"
                        added_layers = [ { tar = "1" } ]
                    "#}),
                    concat!(
                        "field `added_layers` cannot be set without ",
                        "`image` or `parent` also being specified (try `layers` instead)",
                    ),
                );
            }

            #[test]
            fn empty_added_layers() {
                assert_eq!(
                    parse_container_spec_json(indoc! {r#"{
                        "parent": "parent",
                        "added_layers": []
                    }"#}),
                    container_spec! {
                        parent: container_container_parent!("parent", all),
                    },
                );
            }

            #[test]
            fn image_with_implicit_layers() {
                assert_eq!(
                    parse_container_spec_toml(indoc! {r#"
                        image = "image1"
                    "#}),
                    container_spec! {
                        parent: image_container_parent!("image1", all),
                    },
                );
            }

            #[test]
            fn image_with_explicit_layers() {
                assert_eq!(
                    parse_container_spec_json(indoc! {r#"{
                        "image": {
                            "name": "image1",
                            "use": [ "layers" ]
                        }
                    }"#}),
                    container_spec! {
                        parent: image_container_parent!("image1", layers),
                    },
                );
            }

            #[test]
            fn parent_with_implicit_layers() {
                assert_eq!(
                    parse_container_spec_toml(indoc! {r#"
                        parent = "parent"
                    "#}),
                    container_spec! {
                        parent: container_container_parent!("parent", all),
                    },
                );
            }

            #[test]
            fn parent_with_explicit_layers() {
                assert_eq!(
                    parse_container_spec_json(indoc! {r#"{
                        "parent": {
                            "name": "parent",
                            "use": [ "layers" ]
                        }
                    }"#}),
                    container_spec! {
                        parent: container_container_parent!("parent", layers),
                    },
                );
            }

            #[test]
            fn layers_and_image_with_implicit_layers() {
                assert_eq!(
                    parse_container_spec_toml(indoc! {r#"
                        image = "image1"
                        layers = [ { tar = "1" } ]
                    "#}),
                    container_spec! {
                        layers: [tar_layer_spec!("1")],
                        parent: image_container_parent!("image1", all, -layers),
                    },
                );
            }

            #[test]
            fn layers_and_image_with_explicit_layers() {
                assert_eq!(
                    parse_container_spec_error_json(indoc! {r#"{
                        "image": {
                            "name": "image1",
                            "use": [ "layers" ]
                        },
                        "layers": [ { "tar": "1" } ]
                    }"#}),
                    concat!(
                        "field `layers` cannot be set if `image` with an explicit `use` of ",
                        "`layers` is also specified (try `added_layers` instead)",
                    ),
                );
            }

            #[test]
            fn layers_and_image_without_layers() {
                assert_eq!(
                    parse_container_spec_toml(indoc! {r#"
                        image = { name = "image1", use = [ "environment" ] }
                        layers = [ { tar = "1" } ]
                    "#}),
                    container_spec! {
                        layers: [tar_layer_spec!("1")],
                        parent: image_container_parent!("image1", environment),
                    },
                );
            }

            #[test]
            fn layers_and_parent_with_implicit_layers() {
                assert_eq!(
                    parse_container_spec_json(indoc! {r#"{
                        "parent": "parent",
                        "layers": [ { "tar": "1" } ]
                    }"#}),
                    container_spec! {
                        layers: [tar_layer_spec!("1")],
                        parent: container_container_parent!("parent", all, -layers),
                    },
                );
            }

            #[test]
            fn layers_and_parent_with_explicit_layers() {
                assert_eq!(
                    parse_container_spec_error_toml(indoc! {r#"
                        parent.name = "parent"
                        parent.use = [ "layers" ]
                        layers = [ { tar = "1" } ]
                    "#}),
                    concat!(
                        "field `layers` cannot be set if `parent` with an explicit `use` of ",
                        "`layers` is also specified (try `added_layers` instead)",
                    ),
                );
            }

            #[test]
            fn layers_and_parent_without_layers() {
                assert_eq!(
                    parse_container_spec_json(indoc! {r#"{
                        "parent": {
                            "name": "parent",
                            "use": [ "environment" ]
                        },
                        "layers": [ { "tar": "1" } ]
                    }"#}),
                    container_spec! {
                        layers: [tar_layer_spec!("1")],
                        parent: container_container_parent!("parent", environment),
                    },
                );
            }

            #[test]
            fn no_layers_and_image_without_layers() {
                assert_eq!(
                    parse_container_spec_toml(indoc! {r#"
                        image.name = "image1"
                        image.use = [ "environment" ]
                    "#}),
                    container_spec! {
                        parent: image_container_parent!("image1", environment),
                    },
                );
            }

            #[test]
            fn no_layers_and_parent_without_layers() {
                assert_eq!(
                    parse_container_spec_json(indoc! {r#"{
                        "parent": {
                            "name": "parent",
                            "use": [ "environment" ]
                        }
                    }"#}),
                    container_spec! {
                        parent: container_container_parent!("parent", environment),
                    },
                );
            }

            #[test]
            fn empty_layers() {
                assert_eq!(
                    parse_container_spec_toml(indoc! {r#"
                        layers = []
                    "#}),
                    container_spec! {},
                );
            }
        }

        mod environment {
            use super::*;

            #[test]
            fn added_environment_and_environment() {
                assert_eq!(
                    parse_container_spec_error_json(indoc! {r#"{
                        "environment": { "FOO": "foo", "BAR": "bar" },
                        "added_environment": { "FROB": "frob" }
                    }"#}),
                    "field `added_environment` cannot be set with `environment` field",
                );
            }

            #[test]
            fn added_environment_and_image_with_implicit_environment() {
                assert_eq!(
                    parse_container_spec_toml(indoc! {r#"
                        image = "image1"
                        added_environment = { FROB = "frob" }
                    "#}),
                    container_spec! {
                        environment: [environment_spec!("FROB" => "frob")],
                        parent: image_container_parent!("image1", all),
                    },
                );
            }

            #[test]
            fn added_environment_and_image_with_explicit_environment() {
                assert_eq!(
                    parse_container_spec_json(indoc! {r#"{
                        "image": {
                            "name": "image1",
                            "use": [ "environment" ]
                        },
                        "added_environment": { "FROB": "frob" }
                    }"#}),
                    container_spec! {
                        environment: [environment_spec!("FROB" => "frob")],
                        parent: image_container_parent!("image1", environment),
                    },
                );
            }

            #[test]
            fn added_environment_and_image_without_environment() {
                assert_eq!(
                    parse_container_spec_error_toml(indoc! {r#"
                        image.name = "image1"
                        image.use = [ "layers" ]
                        added_environment = { FROB = "frob" }
                    "#}),
                    concat!(
                        "field `added_environment` requires `image` being specified with a ",
                        "`use` of `environment` (try `environment` instead)",
                    ),
                );
            }

            #[test]
            fn added_environment_and_parent_with_implicit_environment() {
                assert_eq!(
                    parse_container_spec_json(indoc! {r#"{
                        "parent": "parent",
                        "added_environment": [
                            {
                                "vars": { "FROB": "frob" },
                                "extend": false
                            },
                            {
                                "vars": { "BAZ": "baz" },
                                "extend": true
                            }
                        ]
                    }"#}),
                    container_spec! {
                        environment: [
                            environment_spec!(extend: false, "FROB" => "frob"),
                            environment_spec!(extend: true, "BAZ" => "baz"),
                        ],
                        parent: container_container_parent!("parent", all),
                    },
                );
            }

            #[test]
            fn added_environment_and_parent_with_explicit_environment() {
                assert_eq!(
                    parse_container_spec_toml(indoc! {r#"
                        parent.name = "parent"
                        parent.use = [ "environment" ]
                        [[added_environment]]
                        vars = { FROB = "frob" }
                        extend = false
                        [[added_environment]]
                        vars = { BAZ = "baz" }
                        extend = true
                    "#}),
                    container_spec! {
                        environment: [
                            environment_spec!(extend: false, "FROB" => "frob"),
                            environment_spec!(extend: true, "BAZ" => "baz"),
                        ],
                        parent: container_container_parent!("parent", environment),
                    },
                );
            }

            #[test]
            fn added_environment_and_parent_without_environment() {
                assert_eq!(
                    parse_container_spec_error_json(indoc! {r#"{
                        "parent": {
                            "name": "parent",
                            "use": [ "layers" ]
                        },
                        "added_environment": [
                            {
                                "vars": { "FROB": "frob" },
                                "extend": false
                            },
                            {
                                "vars": { "BAZ": "baz" },
                                "extend": true
                            }
                        ]
                    }"#}),
                    concat!(
                        "field `added_environment` requires `parent` being specified with a ",
                        "`use` of `environment` (try `environment` instead)",
                    ),
                );
            }

            #[test]
            fn added_environment_and_neither_image_nor_parent() {
                assert_eq!(
                    parse_container_spec_error_toml(indoc! {r#"
                        added_environment = { FROB = "frob" }
                    "#}),
                    concat!(
                        "field `added_environment` cannot be set without ",
                        "`image` or `parent` also being specified (try `environment` instead)",
                    ),
                );
            }

            #[test]
            fn empty_added_environment() {
                assert_eq!(
                    parse_container_spec_json(indoc! {r#"{
                        "parent": "parent",
                        "added_environment": []
                    }"#}),
                    container_spec! {
                        parent: container_container_parent!("parent", all),
                    },
                );
            }

            #[test]
            fn image_with_implicit_environment() {
                assert_eq!(
                    parse_container_spec_toml(indoc! {r#"
                        image = "image1"
                    "#}),
                    container_spec! {
                        parent: image_container_parent!("image1", all),
                    },
                );
            }

            #[test]
            fn image_with_explicit_environment() {
                assert_eq!(
                    parse_container_spec_json(indoc! {r#"{
                        "image": {
                            "name": "image1",
                            "use": [ "environment" ]
                        }
                    }"#}),
                    container_spec! {
                        parent: image_container_parent!("image1", environment),
                    },
                );
            }

            #[test]
            fn parent_with_implicit_environment() {
                assert_eq!(
                    parse_container_spec_toml(indoc! {r#"
                        parent = "parent"
                    "#}),
                    container_spec! {
                        parent: container_container_parent!("parent", all),
                    },
                );
            }

            #[test]
            fn parent_with_explicit_environment() {
                assert_eq!(
                    parse_container_spec_json(indoc! {r#"{
                        "parent": {
                            "name": "parent",
                            "use": [ "environment" ]
                        }
                    }"#}),
                    container_spec! {
                        parent: container_container_parent!("parent", environment),
                    },
                );
            }

            #[test]
            fn environment_and_image_with_implicit_environment() {
                assert_eq!(
                    parse_container_spec_toml(indoc! {r#"
                        image = "image1"
                        environment = { FROB = "frob" }
                    "#}),
                    container_spec! {
                        environment: [environment_spec!("FROB" => "frob")],
                        parent: image_container_parent!("image1", all, -environment),
                    },
                );
            }

            #[test]
            fn environment_and_image_with_explicit_environment() {
                assert_eq!(
                    parse_container_spec_error_json(indoc! {r#"{
                        "image": {
                            "name": "image1",
                            "use": [ "environment" ]
                        },
                        "environment": { "FROB": "frob" }
                    }"#}),
                    concat!(
                        "field `environment` cannot be set if `image` with an explicit `use` of ",
                        "`environment` is also specified (try `added_environment` instead)",
                    ),
                );
            }

            #[test]
            fn environment_and_image_without_environment() {
                assert_eq!(
                    parse_container_spec_toml(indoc! {r#"
                        image.name = "image1"
                        image.use = [ "layers" ]
                        environment = { FROB = "frob" }
                    "#}),
                    container_spec! {
                        environment: [environment_spec!("FROB" => "frob")],
                        parent: image_container_parent!("image1", layers),
                    },
                );
            }

            #[test]
            fn environment_and_parent_with_implicit_environment() {
                assert_eq!(
                    parse_container_spec_json(indoc! {r#"{
                        "parent": "parent",
                        "environment": [
                            {
                                "vars": { "FROB": "frob" },
                                "extend": false
                            },
                            {
                                "vars": { "BAZ": "baz" },
                                "extend": true
                            }
                        ]
                    }"#}),
                    container_spec! {
                        environment: [
                            environment_spec!(extend: false, "FROB" => "frob"),
                            environment_spec!(extend: true, "BAZ" => "baz"),
                        ],
                        parent: container_container_parent!("parent", all, -environment),
                    },
                );
            }

            #[test]
            fn environment_and_parent_with_explicit_environment() {
                assert_eq!(
                    parse_container_spec_error_toml(indoc! {r#"
                        [parent]
                        name = "parent"
                        use = [ "environment" ]

                        [[environment]]
                        vars = { FROB = "frob" }
                        extend = false

                        [[environment]]
                        vars = { BAZ = "baz" }
                        extend = true
                    "#}),
                    concat!(
                        "field `environment` cannot be set if `parent` with an explicit `use` of ",
                        "`environment` is also specified (try `added_environment` instead)",
                    ),
                );
            }

            #[test]
            fn environment_and_parent_without_environment() {
                assert_eq!(
                    parse_container_spec_json(indoc! {r#"{
                        "parent": {
                            "name": "parent",
                            "use": [ "layers" ]
                        },
                        "environment": [
                            {
                                "vars": { "FROB": "frob" },
                                "extend": false
                            },
                            {
                                "vars": { "BAZ": "baz" },
                                "extend": true
                            }
                        ]
                    }"#}),
                    container_spec! {
                        environment: [
                            environment_spec!(extend: false, "FROB" => "frob"),
                            environment_spec!(extend: true, "BAZ" => "baz"),
                        ],
                        parent: container_container_parent!("parent", layers),
                    },
                );
            }

            #[test]
            fn no_environment_and_image_without_environment() {
                assert_eq!(
                    parse_container_spec_toml(indoc! {r#"
                        [image]
                        name = "image1"
                        use = [ "layers" ]
                    "#}),
                    container_spec! {
                        parent: image_container_parent!("image1", layers),
                    },
                );
            }

            #[test]
            fn no_environment_and_parent_without_environment() {
                assert_eq!(
                    parse_container_spec_json(indoc! {r#"{
                        "parent": {
                            "name": "parent",
                            "use": [ "layers" ]
                        }
                    }"#}),
                    container_spec! {
                        parent: container_container_parent!("parent", layers),
                    },
                );
            }

            #[test]
            fn empty_environment() {
                assert_eq!(
                    parse_container_spec_toml(indoc! {r#"
                        environment = []
                    "#}),
                    container_spec! {},
                );
            }
        }

        mod working_directory {
            use super::*;

            #[test]
            fn working_directory() {
                assert_eq!(
                    parse_container_spec_json(indoc! {r#"{
                        "layers": [ { "tar": "1" } ],
                        "working_directory": "/foo/bar"
                    }"#}),
                    container_spec! {
                        layers: [tar_layer_spec!("1")],
                        working_directory: "/foo/bar",
                    },
                )
            }

            #[test]
            fn image_with_implicit_working_directory() {
                assert_eq!(
                    parse_container_spec_toml(indoc! {r#"
                        image = "image1"
                    "#}),
                    container_spec! {
                        parent: image_container_parent!("image1", all),
                    },
                )
            }

            #[test]
            fn image_with_explicit_working_directory() {
                assert_eq!(
                    parse_container_spec_json(indoc! {r#"{
                        "layers": [ { "tar": "1" } ],
                        "image": {
                            "name": "image1",
                            "use": [ "working_directory" ]
                        }
                    }"#}),
                    container_spec! {
                        layers: [tar_layer_spec!("1")],
                        parent: image_container_parent!("image1", working_directory),
                    },
                )
            }

            #[test]
            fn working_directory_and_image_with_implicit_working_directory() {
                assert_eq!(
                    parse_container_spec_toml(indoc! {r#"
                        working_directory = "/foo/bar"
                        image = "image1"
                    "#}),
                    container_spec! {
                        working_directory: "/foo/bar",
                        parent: image_container_parent!("image1", all, -working_directory),
                    },
                )
            }

            #[test]
            fn working_directory_and_image_with_explicit_working_directory() {
                assert_eq!(
                    parse_container_spec_error_json(indoc! {r#"{
                        "layers": [ { "tar": "1" } ],
                        "working_directory": "/foo/bar",
                        "image": {
                            "name": "image1",
                            "use": [ "working_directory" ]
                        }
                    }"#}),
                    concat!(
                        "field `working_directory` cannot be set if `image` with an explicit `use` of ",
                        "`working_directory` is also specified",
                    ),
                )
            }

            #[test]
            fn working_directory_and_image_without_working_directory() {
                assert_eq!(
                    parse_container_spec_toml(indoc! {r#"
                        layers = [ { tar = "1" } ]
                        working_directory = "/foo/bar"
                        image.name = "image1"
                        image.use = [ "environment" ]
                    "#}),
                    container_spec! {
                        layers: [tar_layer_spec!("1")],
                        working_directory: "/foo/bar",
                        parent: image_container_parent!("image1", environment),
                    },
                )
            }

            #[test]
            fn parent_with_implicit_working_directory() {
                assert_eq!(
                    parse_container_spec_json(indoc! {r#"{
                        "parent": "parent"
                    }"#}),
                    container_spec! {
                        parent: container_container_parent!("parent", all),
                    },
                )
            }

            #[test]
            fn parent_with_explicit_working_directory() {
                assert_eq!(
                    parse_container_spec_toml(indoc! {r#"
                        layers = [ { tar = "1" } ]
                        parent.name = "parent"
                        parent.use = [ "working_directory" ]
                    "#}),
                    container_spec! {
                        layers: [tar_layer_spec!("1")],
                        parent: container_container_parent!("parent", working_directory),
                    },
                )
            }

            #[test]
            fn working_directory_and_parent_with_implicit_working_directory() {
                assert_eq!(
                    parse_container_spec_json(indoc! {r#"{
                        "working_directory": "/foo/bar",
                        "parent": "parent"
                    }"#}),
                    container_spec! {
                        working_directory: "/foo/bar",
                        parent: container_container_parent!("parent", all, -working_directory),
                    },
                )
            }

            #[test]
            fn working_directory_and_parent_with_explicit_working_directory() {
                assert_eq!(
                    parse_container_spec_error_toml(indoc! {r#"
                        layers = [ { tar = "1" } ]
                        working_directory = "/foo/bar"
                        parent.name = "parent"
                        parent.use = [ "working_directory" ]
                    "#}),
                    concat!(
                        "field `working_directory` cannot be set if `parent` with an explicit `use` of ",
                        "`working_directory` is also specified",
                    ),
                )
            }

            #[test]
            fn working_directory_and_parent_without_working_directory() {
                assert_eq!(
                    parse_container_spec_json(indoc! {r#"{
                        "layers": [ { "tar": "1" } ],
                        "working_directory": "/foo/bar",
                        "parent": {
                            "name": "parent",
                            "use": [ "environment" ]
                        }
                    }"#}),
                    container_spec! {
                        layers: [tar_layer_spec!("1")],
                        working_directory: "/foo/bar",
                        parent: container_container_parent!("parent", environment),
                    },
                )
            }
        }

        mod enable_writable_file_system {
            use super::*;

            #[test]
            fn enable_writable_file_system() {
                assert_eq!(
                    parse_container_spec_toml(indoc! {r#"
                        enable_writable_file_system = true
                    "#}),
                    container_spec! {
                        enable_writable_file_system: true,
                    },
                )
            }

            #[test]
            fn parent_with_implicit_enable_writable_file_system() {
                assert_eq!(
                    parse_container_spec_json(indoc! {r#"{
                        "parent": "parent"
                    }"#}),
                    container_spec! {
                        parent: container_container_parent!("parent", all),
                    },
                )
            }

            #[test]
            fn parent_with_explicit_enable_writable_file_system() {
                assert_eq!(
                    parse_container_spec_toml(indoc! {r#"
                        [parent]
                        name = "parent"
                        use = [ "enable_writable_file_system" ]
                    "#}),
                    container_spec! {
                        parent: container_container_parent!("parent", enable_writable_file_system),
                    },
                )
            }

            #[test]
            fn enable_writable_file_system_and_parent_with_implicit_enable_writable_file_system() {
                assert_eq!(
                    parse_container_spec_json(indoc! {r#"{
                        "enable_writable_file_system": true,
                        "parent": "parent"
                    }"#}),
                    container_spec! {
                        enable_writable_file_system: true,
                        parent: container_container_parent!("parent", all, -enable_writable_file_system),
                    },
                )
            }

            #[test]
            fn enable_writable_file_system_and_parent_with_explicit_enable_writable_file_system() {
                assert_eq!(
                    parse_container_spec_error_toml(indoc! {r#"
                        enable_writable_file_system = true
                        parent.name = "parent"
                        parent.use = [ "enable_writable_file_system" ]
                    "#}),
                    concat!(
                        "field `enable_writable_file_system` cannot be set if `parent` with an explicit `use` of ",
                        "`enable_writable_file_system` is also specified",
                    ),
                )
            }

            #[test]
            fn enable_writable_file_system_and_parent_without_enable_writable_file_system() {
                assert_eq!(
                    parse_container_spec_json(indoc! {r#"{
                        "enable_writable_file_system": true,
                        "parent": {
                            "name": "parent",
                            "use": [ "environment" ]
                        }
                    }"#}),
                    container_spec! {
                        enable_writable_file_system: true,
                        parent: container_container_parent!("parent", environment),
                    },
                )
            }
        }

        mod mounts {
            use super::*;

            #[test]
            fn mounts() {
                assert_eq!(
                    parse_container_spec_toml(indoc! {r#"
                        layers = [ { tar = "1" } ]
                        mounts = [
                            { type = "tmp", mount_point = "/tmp" },
                            { type = "bind", mount_point = "/bind", local_path = "/a" },
                            { type = "bind", mount_point = "/bind2", local_path = "/b", read_only = false },
                            { type = "bind", mount_point = "/bind3", local_path = "/c", read_only = true },
                        ]
                    "#}),
                    container_spec! {
                        layers: [tar_layer_spec!("1")],
                        mounts: [
                            JobMount::Tmp { mount_point: utf8_path_buf!("/tmp") },
                            JobMount::Bind {
                                mount_point: utf8_path_buf!("/bind"),
                                local_path: utf8_path_buf!("/a"),
                                read_only: false,
                            },
                            JobMount::Bind {
                                mount_point: utf8_path_buf!("/bind2"),
                                local_path: utf8_path_buf!("/b"),
                                read_only: false,
                            },
                            JobMount::Bind {
                                mount_point: utf8_path_buf!("/bind3"),
                                local_path: utf8_path_buf!("/c"),
                                read_only: true,
                            },
                        ],
                    },
                )
            }

            #[test]
            fn added_mounts_and_mounts() {
                assert_eq!(
                    parse_container_spec_error_json(indoc! {r#"{
                        "mounts": [{ "type": "proc", "mount_point": "/proc" }],
                        "added_mounts": [{ "type": "tmp", "mount_point": "/tmp" }]
                    }"#}),
                    "field `added_mounts` cannot be set with `mounts` field",
                );
            }

            #[test]
            fn added_mounts_and_parent_with_implicit_mounts() {
                assert_eq!(
                    parse_container_spec_toml(indoc! {r#"
                        parent = "parent"
                        added_mounts = [{ type = "tmp", mount_point = "/tmp" }]
                    "#}),
                    container_spec! {
                        mounts: [tmp_mount!("/tmp")],
                        parent: container_container_parent!("parent", all),
                    },
                );
            }

            #[test]
            fn added_mounts_and_parent_with_explicit_mounts() {
                assert_eq!(
                    parse_container_spec_json(indoc! {r#"{
                        "parent": {
                            "name": "parent",
                            "use": [ "mounts" ]
                        },
                        "added_mounts": [{ "type": "tmp", "mount_point": "/tmp" }]
                    }"#}),
                    container_spec! {
                        mounts: [tmp_mount!("/tmp")],
                        parent: container_container_parent!("parent", mounts),
                    },
                );
            }

            #[test]
            fn added_mounts_and_parent_without_mounts() {
                assert_eq!(
                    parse_container_spec_error_toml(indoc! {r#"
                        parent.name = "parent"
                        parent.use = [ "environment" ]
                        added_mounts = [{ type = "tmp", mount_point = "/tmp" }]
                    "#}),
                    concat!(
                        "field `added_mounts` requires `parent` being specified with a ",
                        "`use` of `mounts` (try `mounts` instead)",
                    ),
                );
            }

            #[test]
            fn added_mounts_and_no_parent() {
                assert_eq!(
                    parse_container_spec_error_json(indoc! {r#"{
                        "added_mounts": [{ "type": "tmp", "mount_point": "/tmp" }]
                    }"#}),
                    concat!(
                        "field `added_mounts` cannot be set without ",
                        "`parent` also being specified (try `mounts` instead)",
                    ),
                );
            }

            #[test]
            fn empty_added_mounts() {
                assert_eq!(
                    parse_container_spec_toml(indoc! {r#"
                        parent = "parent"
                        added_mounts = []
                    "#}),
                    container_spec! {
                        parent: container_container_parent!("parent", all),
                    },
                );
            }

            #[test]
            fn parent_with_implicit_mounts() {
                assert_eq!(
                    parse_container_spec_json(indoc! {r#"{
                        "parent": "parent"
                    }"#}),
                    container_spec! {
                        parent: container_container_parent!("parent", all),
                    },
                );
            }

            #[test]
            fn parent_with_explicit_mounts() {
                assert_eq!(
                    parse_container_spec_toml(indoc! {r#"
                        [parent]
                        name = "parent"
                        use = [ "mounts" ]
                    "#}),
                    container_spec! {
                        parent: container_container_parent!("parent", mounts),
                    },
                );
            }

            #[test]
            fn mounts_and_parent_with_implicit_mounts() {
                assert_eq!(
                    parse_container_spec_json(indoc! {r#"{
                        "parent": "parent",
                        "mounts": [{ "type": "proc", "mount_point": "/proc" }]
                    }"#}),
                    container_spec! {
                        mounts: [proc_mount!("/proc")],
                        parent: container_container_parent!("parent", all, -mounts),
                    },
                );
            }

            #[test]
            fn mounts_and_parent_with_explicit_mounts() {
                assert_eq!(
                    parse_container_spec_error_toml(indoc! {r#"
                        parent.name = "parent"
                        parent.use = [ "mounts" ]
                        mounts = [{ type = "proc", mount_point = "/proc" }]
                    "#}),
                    concat!(
                        "field `mounts` cannot be set if `parent` with an explicit `use` of ",
                        "`mounts` is also specified (try `added_mounts` instead)",
                    ),
                );
            }

            #[test]
            fn mounts_and_parent_without_mounts() {
                assert_eq!(
                    parse_container_spec_json(indoc! {r#"{
                        "parent": {
                            "name": "parent",
                            "use": [ "environment" ]
                        },
                        "mounts": [{ "type": "proc", "mount_point": "/proc" }]
                    }"#}),
                    container_spec! {
                        mounts: [proc_mount!("/proc")],
                        parent: container_container_parent!("parent", environment),
                    },
                );
            }

            #[test]
            fn no_mounts_and_parent_without_mounts() {
                assert_eq!(
                    parse_container_spec_toml(indoc! {r#"
                        parent.name = "parent"
                        parent.use =  [ "environment" ]
                    "#}),
                    container_spec! {
                        parent: container_container_parent!("parent", environment),
                    },
                );
            }

            #[test]
            fn empty_mounts() {
                assert_eq!(
                    parse_container_spec_json(indoc! {r#"{
                        "mounts": []
                    }"#}),
                    container_spec! {},
                );
            }
        }

        mod network {
            use super::*;

            #[test]
            fn network() {
                assert_eq!(
                    parse_container_spec_toml(indoc! {r#"
                        network = "loopback"
                    "#}),
                    container_spec! {
                        network: JobNetwork::Loopback,
                    },
                )
            }

            #[test]
            fn parent_with_implicit_network() {
                assert_eq!(
                    parse_container_spec_json(indoc! {r#"{
                        "parent": "parent"
                    }"#}),
                    container_spec! {
                        parent: container_container_parent!("parent", all),
                    },
                )
            }

            #[test]
            fn parent_with_explicit_network() {
                assert_eq!(
                    parse_container_spec_toml(indoc! {r#"
                        [parent]
                        name = "parent"
                        use = [ "network" ]
                    "#}),
                    container_spec! {
                        parent: container_container_parent!("parent", network),
                    },
                )
            }

            #[test]
            fn network_and_parent_with_implicit_network() {
                assert_eq!(
                    parse_container_spec_json(indoc! {r#"{
                        "network": "loopback",
                        "parent": "parent"
                    }"#}),
                    container_spec! {
                        network: JobNetwork::Loopback,
                        parent: container_container_parent!("parent", all, -network),
                    },
                )
            }

            #[test]
            fn network_and_parent_with_explicit_network() {
                assert_eq!(
                    parse_container_spec_error_toml(indoc! {r#"
                        network = "loopback"
                        parent.name = "parent"
                        parent.use = [ "network" ]
                    "#}),
                    concat!(
                        "field `network` cannot be set if `parent` with an explicit `use` of ",
                        "`network` is also specified",
                    ),
                )
            }

            #[test]
            fn network_and_parent_without_network() {
                assert_eq!(
                    parse_container_spec_json(indoc! {r#"{
                        "network": "loopback",
                        "parent": {
                            "name": "parent",
                            "use": [ "environment" ]
                        }
                    }"#}),
                    container_spec! {
                        network: JobNetwork::Loopback,
                        parent: container_container_parent!("parent", environment),
                    },
                )
            }
        }

        mod user {
            use super::*;

            #[test]
            fn user() {
                assert_eq!(
                    parse_container_spec_toml(indoc! {r#"
                        user = 1234
                    "#}),
                    container_spec! {
                        user: 1234,
                    },
                )
            }

            #[test]
            fn parent_with_implicit_user() {
                assert_eq!(
                    parse_container_spec_json(indoc! {r#"{
                        "parent": "parent"
                    }"#}),
                    container_spec! {
                        parent: container_container_parent!("parent", all),
                    },
                )
            }

            #[test]
            fn parent_with_explicit_user() {
                assert_eq!(
                    parse_container_spec_toml(indoc! {r#"
                        [parent]
                        name = "parent"
                        use = [ "user" ]
                    "#}),
                    container_spec! {
                        parent: container_container_parent!("parent", user),
                    },
                )
            }

            #[test]
            fn user_and_parent_with_implicit_user() {
                assert_eq!(
                    parse_container_spec_json(indoc! {r#"{
                        "user": 101,
                        "parent": "parent"
                    }"#}),
                    container_spec! {
                        user: 101,
                        parent: container_container_parent!("parent", all, -user),
                    },
                )
            }

            #[test]
            fn user_and_parent_with_explicit_user() {
                assert_eq!(
                    parse_container_spec_error_toml(indoc! {r#"
                        user = 101
                        parent.name = "parent"
                        parent.use = [ "user" ]
                    "#}),
                    concat!(
                        "field `user` cannot be set if `parent` with an explicit `use` of ",
                        "`user` is also specified",
                    ),
                )
            }

            #[test]
            fn user_and_parent_without_user() {
                assert_eq!(
                    parse_container_spec_json(indoc! {r#"{
                        "user": 101,
                        "parent": {
                            "name": "parent",
                            "use": [ "environment" ]
                        }
                    }"#}),
                    container_spec! {
                        user: 101,
                        parent: container_container_parent!("parent", environment),
                    },
                )
            }
        }

        mod group {
            use super::*;
            #[test]
            fn group() {
                assert_eq!(
                    parse_container_spec_toml(indoc! {r#"
                        group = 4321
                    "#}),
                    container_spec! {
                        group: 4321,
                    },
                )
            }

            #[test]
            fn parent_with_implicit_group() {
                assert_eq!(
                    parse_container_spec_json(indoc! {r#"{
                        "parent": "parent"
                    }"#}),
                    container_spec! {
                        parent: container_container_parent!("parent", all),
                    },
                )
            }

            #[test]
            fn parent_with_explicit_group() {
                assert_eq!(
                    parse_container_spec_toml(indoc! {r#"
                        [parent]
                        name = "parent"
                        use = [ "group" ]
                    "#}),
                    container_spec! {
                        parent: container_container_parent!("parent", group),
                    },
                )
            }

            #[test]
            fn group_and_parent_with_implicit_group() {
                assert_eq!(
                    parse_container_spec_json(indoc! {r#"{
                        "group": 101,
                        "parent": "parent"
                    }"#}),
                    container_spec! {
                        group: 101,
                        parent: container_container_parent!("parent", all, -group),
                    },
                )
            }

            #[test]
            fn group_and_parent_with_explicit_group() {
                assert_eq!(
                    parse_container_spec_error_toml(indoc! {r#"
                        group = 101
                        parent.name = "parent"
                        parent.use = [ "group" ]
                    "#}),
                    concat!(
                        "field `group` cannot be set if `parent` with an explicit `use` of ",
                        "`group` is also specified",
                    ),
                )
            }

            #[test]
            fn group_and_parent_without_group() {
                assert_eq!(
                    parse_container_spec_json(indoc! {r#"{
                        "group": 101,
                        "parent": {
                            "name": "parent",
                            "use": [ "environment" ]
                        }
                    }"#}),
                    container_spec! {
                        group: 101,
                        parent: container_container_parent!("parent", environment),
                    },
                )
            }
        }
    }

    mod job_spec {
        use super::*;

        #[track_caller]
        fn parse_job_spec_toml(file: &str) -> JobSpec {
            toml::from_str(file).unwrap()
        }

        #[track_caller]
        fn parse_job_spec_json(file: &str) -> JobSpec {
            serde_json::from_str(file).unwrap()
        }

        #[track_caller]
        fn parse_job_spec_error_toml(file: &str) -> String {
            format!("{}", toml::from_str::<JobSpec>(file).unwrap_err())
                .trim_end()
                .into()
        }

        #[track_caller]
        fn parse_job_spec_error_json(file: &str) -> String {
            format!("{}", serde_json::from_str::<JobSpec>(file).unwrap_err())
        }

        #[test]
        fn empty() {
            assert_eq!(
                parse_job_spec_error_json("{}"),
                "missing field `program` at line 1 column 2",
            );
        }

        #[test]
        fn unknown_field() {
            assert!(parse_job_spec_error_toml(indoc! {r#"
                program = "/bin/sh"
                foo_bar_baz = 3
            "#})
            .contains("unknown field `foo_bar_baz`"));
        }

        #[test]
        fn program() {
            assert_eq!(
                parse_job_spec_toml(indoc! {r#"
                    program = "/bin/sh"
                "#}),
                job_spec! {
                    "/bin/sh",
                },
            );
        }

        #[test]
        fn arguments() {
            assert_eq!(
                parse_job_spec_json(indoc! {r#"{
                    "program": "/bin/sh",
                    "arguments": ["foo", "bar"]
                }"#}),
                job_spec! {
                    "/bin/sh",
                    arguments: [ "foo", "bar" ],
                },
            );
        }

        #[test]
        fn empty_arguments() {
            assert_eq!(
                parse_job_spec_toml(indoc! {r#"
                    program = "/bin/sh"
                    arguments = []
                "#}),
                job_spec! {
                    "/bin/sh",
                },
            );
        }

        #[test]
        fn timeout() {
            assert_eq!(
                parse_job_spec_json(indoc! {r#"{
                    "program": "/bin/sh",
                    "timeout": 42
                }"#}),
                job_spec! {
                    "/bin/sh",
                    timeout: 42,
                },
            );
        }

        #[test]
        fn zero_timeout() {
            assert_eq!(
                parse_job_spec_toml(indoc! {r#"
                    program = "/bin/sh"
                    timeout = 0
                "#}),
                job_spec! {
                    "/bin/sh",
                },
            );
        }

        #[test]
        fn priority() {
            assert_eq!(
                parse_job_spec_json(indoc! {r#"{
                    "program": "/bin/sh",
                    "priority": 42
                }"#}),
                job_spec! {
                    "/bin/sh",
                    priority: 42,
                },
            );
        }

        #[test]
        fn container_fields() {
            assert_eq!(
                parse_job_spec_toml(indoc! {r#"
                    program = "/bin/sh"
                    parent = "parent"
                    layers = [ { tar = "1" }, { tar = "2" } ]
                    added_environment = { FOO = "foo" }
                    working_directory = "/root"
                "#}),
                job_spec! {
                    "/bin/sh",
                    layers: [tar_layer_spec!("1"), tar_layer_spec!("2")],
                    environment: [environment_spec!("FOO" => "foo")],
                    working_directory: "/root",
                    parent: container_container_parent!("parent", all, -layers, -working_directory),
                },
            );
        }
    }
}
