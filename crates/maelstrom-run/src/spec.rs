use anyhow::{anyhow, Error, Result};
use maelstrom_base::{
    ArtifactType, EnumSet, GroupId, JobDevice, JobDeviceListDeserialize, JobMount, NonEmpty,
    Sha256Digest, Timeout, UserId, Utf8PathBuf,
};
use maelstrom_client::spec::{
    incompatible, EnvironmentSpec, Image, ImageConfig, ImageOption, ImageUse, IntoEnvironment,
    JobSpec, Layer, PossiblyImage,
};
use serde::de::Error as _;
use serde::{de, Deserialize, Deserializer};
use std::collections::BTreeMap;
use std::io::Read;

struct JobSpecIterator<InnerT, LayerMapperT, ImageLookupT> {
    inner: InnerT,
    layer_mapper: LayerMapperT,
    image_lookup: ImageLookupT,
}

impl<InnerT, LayerMapperT, ImageLookupT> Iterator
    for JobSpecIterator<InnerT, LayerMapperT, ImageLookupT>
where
    InnerT: Iterator<Item = serde_json::Result<Job>>,
    LayerMapperT: Fn(Layer) -> Result<(Sha256Digest, ArtifactType)>,
    ImageLookupT: FnMut(&str) -> Result<ImageConfig>,
{
    type Item = Result<JobSpec>;

    fn next(&mut self) -> Option<Self::Item> {
        match self.inner.next() {
            None => None,
            Some(Err(err)) => Some(Err(Error::new(err))),
            Some(Ok(job)) => Some(job.into_job_spec(&self.layer_mapper, &mut self.image_lookup)),
        }
    }
}

pub fn job_spec_iter_from_reader(
    reader: impl Read,
    layer_mapper: impl Fn(Layer) -> Result<(Sha256Digest, ArtifactType)>,
    image_lookup: impl FnMut(&str) -> Result<ImageConfig>,
) -> impl Iterator<Item = Result<JobSpec>> {
    let inner = serde_json::Deserializer::from_reader(reader).into_iter::<Job>();
    JobSpecIterator {
        inner,
        layer_mapper,
        image_lookup,
    }
}

#[derive(Debug, Eq, PartialEq)]
struct Job {
    program: Utf8PathBuf,
    arguments: Option<Vec<String>>,
    environment: Option<Vec<EnvironmentSpec>>,
    use_image_environment: bool,
    layers: PossiblyImage<NonEmpty<Layer>>,
    added_layers: Vec<Layer>,
    devices: Option<EnumSet<JobDeviceListDeserialize>>,
    mounts: Option<Vec<JobMount>>,
    enable_loopback: Option<bool>,
    enable_writable_file_system: Option<bool>,
    working_directory: Option<PossiblyImage<Utf8PathBuf>>,
    user: Option<UserId>,
    group: Option<GroupId>,
    image: Option<String>,
    timeout: Option<u32>,
}

impl Job {
    #[cfg(test)]
    fn new(program: Utf8PathBuf, layers: NonEmpty<Layer>) -> Self {
        Job {
            program,
            layers: PossiblyImage::Explicit(layers),
            added_layers: Default::default(),
            arguments: None,
            environment: None,
            use_image_environment: false,
            devices: None,
            mounts: None,
            enable_loopback: None,
            enable_writable_file_system: None,
            working_directory: None,
            user: None,
            group: None,
            image: None,
            timeout: None,
        }
    }

    fn into_job_spec(
        self,
        layer_mapper: impl Fn(Layer) -> Result<(Sha256Digest, ArtifactType)>,
        image_lookup: impl FnMut(&str) -> Result<ImageConfig>,
    ) -> Result<JobSpec> {
        let image = ImageOption::new(&self.image, image_lookup)?;
        let mut environment = self.environment.unwrap_or_default();
        if self.use_image_environment {
            environment.insert(
                0,
                EnvironmentSpec {
                    vars: image.environment()?,
                    extend: false,
                },
            );
        }
        let mut layers = match self.layers {
            PossiblyImage::Explicit(layers) => layers,
            PossiblyImage::Image => NonEmpty::from_vec(image.layers()?.collect())
                .ok_or_else(|| anyhow!("image {} has no layers to use", image.name()))?,
        };
        layers.extend(self.added_layers);
        let layers = layers.try_map(layer_mapper)?;
        let working_directory = match self.working_directory {
            None => Utf8PathBuf::from("/"),
            Some(PossiblyImage::Explicit(working_directory)) => working_directory,
            Some(PossiblyImage::Image) => image.working_directory()?,
        };
        Ok(JobSpec {
            program: self.program,
            arguments: self.arguments.unwrap_or_default(),
            image: None,
            environment,
            layers: layers.into(),
            devices: self
                .devices
                .unwrap_or(EnumSet::EMPTY)
                .into_iter()
                .map(JobDevice::from)
                .collect(),
            mounts: self.mounts.unwrap_or_default(),
            enable_loopback: self.enable_loopback.unwrap_or_default(),
            enable_writable_file_system: self.enable_writable_file_system.unwrap_or_default(),
            working_directory: Some(working_directory),
            user: self.user.unwrap_or(UserId::from(0)),
            group: self.group.unwrap_or(GroupId::from(0)),
            timeout: self.timeout.and_then(Timeout::new),
        })
    }
}

#[derive(Deserialize)]
#[serde(field_identifier, rename_all = "snake_case")]
enum JobField {
    Program,
    Arguments,
    Environment,
    Layers,
    AddedLayers,
    Devices,
    Mounts,
    EnableLoopback,
    EnableWritableFileSystem,
    WorkingDirectory,
    User,
    Group,
    Image,
    Timeout,
}

struct JobVisitor;

fn must_be_image<T, E>(
    var: &Option<PossiblyImage<T>>,
    if_none: &str,
    if_explicit: &str,
) -> std::result::Result<(), E>
where
    E: de::Error,
{
    match var {
        None => Err(E::custom(format_args!("{}", if_none))),
        Some(PossiblyImage::Explicit(_)) => Err(E::custom(format_args!("{}", if_explicit))),
        Some(PossiblyImage::Image) => Ok(()),
    }
}

#[derive(Deserialize)]
#[serde(untagged)]
enum EnvSelector {
    Implicit(BTreeMap<String, String>),
    Explicit(Vec<EnvironmentSpec>),
}

impl IntoEnvironment for EnvSelector {
    fn into_environment(self) -> Vec<EnvironmentSpec> {
        match self {
            Self::Implicit(v) => v.into_environment(),
            Self::Explicit(v) => v,
        }
    }
}

impl<'de> de::Visitor<'de> for JobVisitor {
    type Value = Job;

    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "Job")
    }

    fn visit_map<A>(self, mut map: A) -> Result<Self::Value, A::Error>
    where
        A: de::MapAccess<'de>,
    {
        let mut program = None;
        let mut arguments = None;
        let mut environment: Option<EnvSelector> = None;
        let mut use_image_environment = false;
        let mut layers = None;
        let mut added_layers = None;
        let mut devices = None;
        let mut mounts = None;
        let mut enable_loopback = None;
        let mut enable_writable_file_system = None;
        let mut working_directory = None;
        let mut user = None;
        let mut group = None;
        let mut image = None;
        let mut timeout = None;
        while let Some(key) = map.next_key()? {
            match key {
                JobField::Program => {
                    program = Some(map.next_value()?);
                }
                JobField::Arguments => {
                    arguments = Some(map.next_value()?);
                }
                JobField::Environment => {
                    environment = Some(map.next_value()?);
                    if matches!(environment, Some(EnvSelector::Implicit(_)))
                        && use_image_environment
                    {
                        return Err(A::Error::custom(concat!(
                            "field `environment` must provide `extend` flags if `image` with a ",
                            "`use` of `environment` is also set"
                        )));
                    }
                }
                JobField::Layers => {
                    incompatible(
                        &layers,
                        concat!(
                            "field `layers` cannot be set if `image` with a `use` of ",
                            "`layers` is also set (try `added_layers` instead)"
                        ),
                    )?;
                    layers = Some(PossiblyImage::Explicit(
                        NonEmpty::from_vec(map.next_value()?).ok_or_else(|| {
                            de::Error::custom(format_args!("field `layers` cannot be empty"))
                        })?,
                    ));
                }
                JobField::AddedLayers => {
                    must_be_image(
                        &layers,
                        "field `added_layers` set before `image` with a `use` of `layers`",
                        "field `added_layers` cannot be set with `layer` field",
                    )?;
                    added_layers = Some(map.next_value()?);
                }
                JobField::Devices => {
                    devices = Some(map.next_value()?);
                }
                JobField::Mounts => {
                    mounts = Some(map.next_value()?);
                }
                JobField::EnableLoopback => {
                    enable_loopback = Some(map.next_value()?);
                }
                JobField::EnableWritableFileSystem => {
                    enable_writable_file_system = Some(map.next_value()?);
                }
                JobField::WorkingDirectory => {
                    incompatible(
                        &working_directory,
                        concat!(
                            "field `working_directory` cannot be set if `image` with a `use` of ",
                            "`working_directory` is also set"
                        ),
                    )?;
                    working_directory = Some(PossiblyImage::Explicit(map.next_value()?));
                }
                JobField::User => {
                    user = Some(map.next_value()?);
                }
                JobField::Group => {
                    group = Some(map.next_value()?);
                }
                JobField::Timeout => {
                    timeout = Some(map.next_value()?);
                }
                JobField::Image => {
                    let i = map.next_value::<Image>()?;
                    image = Some(i.name);
                    for use_ in i.use_ {
                        match use_ {
                            ImageUse::WorkingDirectory => {
                                incompatible(
                                    &working_directory,
                                    concat!(
                                        "field `image` cannot use `working_directory` if field ",
                                        "`working_directory` is also set",
                                    ),
                                )?;
                                working_directory = Some(PossiblyImage::Image);
                            }
                            ImageUse::Layers => {
                                incompatible(
                                    &layers,
                                    concat!(
                                        "field `image` cannot use `layers` if field `layers` ",
                                        "is also set"
                                    ),
                                )?;
                                layers = Some(PossiblyImage::Image);
                            }
                            ImageUse::Environment => {
                                use_image_environment = true;
                                if matches!(environment, Some(EnvSelector::Implicit(_))) {
                                    return Err(A::Error::custom(concat!(
                                        "field `image` cannot use `environment` if `environment` ",
                                        "has not provided `extend` flags"
                                    )));
                                }
                            }
                        }
                    }
                }
            }
        }
        Ok(Job {
            program: program.ok_or_else(|| de::Error::missing_field("program"))?,
            arguments,
            environment: environment.map(|e| e.into_environment()),
            use_image_environment,
            layers: layers.ok_or_else(|| de::Error::missing_field("layers"))?,
            added_layers: added_layers.unwrap_or_default(),
            devices,
            mounts,
            enable_loopback,
            enable_writable_file_system,
            working_directory,
            user,
            group,
            image,
            timeout,
        })
    }
}

impl<'de> de::Deserialize<'de> for Job {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_any(JobVisitor)
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use assert_matches::assert_matches;
    use maelstrom_base::{enum_set, nonempty, JobMountFsType};
    use maelstrom_test::{digest, path_buf_vec, string, string_vec, tar_layer, utf8_path_buf};
    use maplit::btreemap;

    fn layer_mapper(layer: Layer) -> Result<(Sha256Digest, ArtifactType)> {
        assert_matches!(layer, Layer::Tar { path } => {
            Ok((
                Sha256Digest::from(path.as_str().parse::<u64>()?),
                ArtifactType::Tar,
            ))
        })
    }

    fn images(name: &str) -> Result<ImageConfig> {
        match name {
            "image1" => Ok(ImageConfig {
                layers: path_buf_vec!["42", "43"],
                working_directory: Some("/foo".into()),
                environment: Some(string_vec!["FOO=image-foo", "BAZ=image-baz",]),
            }),
            "image-with-env-substitutions" => Ok(ImageConfig {
                environment: Some(string_vec!["PATH=$env{PATH}"]),
                ..Default::default()
            }),
            "empty" => Ok(Default::default()),
            _ => Err(anyhow!("no container named {name} found")),
        }
    }

    #[test]
    fn minimum_into_job_spec() {
        assert_eq!(
            Job::new(utf8_path_buf!("program"), nonempty![tar_layer!("1")])
                .into_job_spec(layer_mapper, images)
                .unwrap(),
            JobSpec::new("program", nonempty![(digest!(1), ArtifactType::Tar)]),
        );
    }

    #[test]
    fn most_into_job_spec() {
        assert_eq!(
            Job {
                arguments: Some(string_vec!["arg1", "arg2"]),
                environment: Some([("FOO", "foo"), ("BAR", "bar")].into_environment()),
                devices: Some(enum_set! {JobDeviceListDeserialize::Null}),
                mounts: Some(vec![JobMount {
                    fs_type: JobMountFsType::Tmp,
                    mount_point: utf8_path_buf!("/tmp"),
                }]),
                working_directory: Some(PossiblyImage::Explicit("/working-directory".into())),
                user: Some(UserId::from(101)),
                group: Some(GroupId::from(202)),
                ..Job::new(utf8_path_buf!("program"), nonempty![tar_layer!("1")])
            }
            .into_job_spec(layer_mapper, images)
            .unwrap(),
            JobSpec::new("program", nonempty![(digest!(1), ArtifactType::Tar)])
                .arguments(["arg1", "arg2"])
                .environment([("BAR", "bar"), ("FOO", "foo")])
                .devices(enum_set! {JobDevice::Null})
                .mounts([JobMount {
                    fs_type: JobMountFsType::Tmp,
                    mount_point: utf8_path_buf!("/tmp"),
                }])
                .working_directory("/working-directory")
                .user(101)
                .group(202),
        );
    }

    #[test]
    fn enable_loopback_into_job_spec() {
        assert_eq!(
            Job {
                enable_loopback: Some(true),
                ..Job::new(utf8_path_buf!("program"), nonempty![tar_layer!("1")])
            }
            .into_job_spec(layer_mapper, images)
            .unwrap(),
            JobSpec::new("program", nonempty![(digest!(1), ArtifactType::Tar)])
                .enable_loopback(true),
        );
    }

    #[test]
    fn enable_writable_file_system_into_job_spec() {
        assert_eq!(
            Job {
                enable_writable_file_system: Some(true),
                ..Job::new(utf8_path_buf!("program"), nonempty![tar_layer!("1")])
            }
            .into_job_spec(layer_mapper, images)
            .unwrap(),
            JobSpec::new("program", nonempty![(digest!(1), ArtifactType::Tar)])
                .enable_writable_file_system(true),
        );
    }

    fn parse_job(str_: &str) -> serde_json::Result<Job> {
        serde_json::from_str(str_)
    }

    fn assert_error(err: serde_json::Error, expected: &str) {
        let message = format!("{err}");
        assert!(
            message.starts_with(expected),
            "message: {message:?}, expected: {expected:?}"
        );
    }

    fn assert_anyhow_error(err: Error, expected: &str) {
        let message = format!("{err}");
        assert!(
            message == expected,
            "message: {message:?}, expected: {expected:?}"
        );
    }

    #[test]
    fn basic() {
        assert_eq!(
            parse_job(
                r#"{
                    "program": "/bin/sh",
                    "layers": [ { "tar": "1" } ]
                }"#
            )
            .unwrap()
            .into_job_spec(layer_mapper, images)
            .unwrap(),
            JobSpec::new(
                string!("/bin/sh"),
                nonempty![(digest!(1), ArtifactType::Tar)]
            ),
        );
    }

    #[test]
    fn missing_program() {
        assert_error(
            parse_job(
                r#"{
                    "layers": [ { "tar": "1" } ]
                }"#,
            )
            .unwrap_err(),
            "missing field `program`",
        );
    }

    #[test]
    fn missing_layers() {
        assert_error(
            parse_job(
                r#"{
                    "program": "/bin/sh"
                }"#,
            )
            .unwrap_err(),
            "missing field `layers`",
        );
    }

    #[test]
    fn empty_layers() {
        assert_error(
            parse_job(
                r#"{
                    "program": "/bin/sh",
                    "layers": []
                }"#,
            )
            .unwrap_err(),
            "field `layers` cannot be empty",
        );
    }

    #[test]
    fn layers_from_image() {
        assert_eq!(
            parse_job(
                r#"{
                    "program": "/bin/sh",
                    "image": {
                        "name": "image1",
                        "use": [ "layers" ]
                    }
                }"#
            )
            .unwrap()
            .into_job_spec(layer_mapper, images)
            .unwrap(),
            JobSpec::new(
                string!("/bin/sh"),
                nonempty![
                    (digest!(42), ArtifactType::Tar),
                    (digest!(43), ArtifactType::Tar)
                ]
            ),
        );
    }

    #[test]
    fn empty_layers_from_image() {
        assert_anyhow_error(
            parse_job(
                r#"{
                    "program": "/bin/sh",
                    "image": {
                        "name": "empty",
                        "use": [ "layers" ]
                    }
                }"#,
            )
            .unwrap()
            .into_job_spec(layer_mapper, images)
            .unwrap_err(),
            "image empty has no layers to use",
        );
    }

    #[test]
    fn layers_after_layers_from_image() {
        assert_error(
            parse_job(
                r#"{
                    "program": "/bin/sh",
                    "image": {
                        "name": "image1",
                        "use": [ "layers" ]
                    },
                    "layers": [ { "tar": "1" } ]
                }"#,
            )
            .unwrap_err(),
            "field `layers` cannot be set if `image` with a `use` of `layers` is also set",
        );
    }

    #[test]
    fn layers_from_image_after_layers() {
        assert_error(
            parse_job(
                r#"{
                    "program": "/bin/sh",
                    "layers": [ { "tar": "1" } ],
                    "image": {
                        "name": "image1",
                        "use": [ "layers" ]
                    }
                }"#,
            )
            .unwrap_err(),
            "field `image` cannot use `layers` if field `layers` is also set",
        );
    }

    #[test]
    fn added_layers_after_layers_from_image() {
        assert_eq!(
            parse_job(
                r#"{
                    "program": "/bin/sh",
                    "image": {
                        "name": "image1",
                        "use": [ "layers" ]
                    },
                    "added_layers": [ { "tar": "1" } ]
                }"#
            )
            .unwrap()
            .into_job_spec(layer_mapper, images)
            .unwrap(),
            JobSpec::new(
                string!("/bin/sh"),
                nonempty![
                    (digest!(42), ArtifactType::Tar),
                    (digest!(43), ArtifactType::Tar),
                    (digest!(1), ArtifactType::Tar)
                ]
            ),
        );
    }

    #[test]
    fn added_layers_only() {
        assert_error(
            parse_job(
                r#"{
                    "program": "/bin/sh",
                    "added_layers": [ { "tar": "1" } ]
                }"#,
            )
            .unwrap_err(),
            "field `added_layers` set before `image` with a `use` of `layers`",
        );
    }

    #[test]
    fn added_layers_before_layers() {
        assert_error(
            parse_job(
                r#"{
                    "program": "/bin/sh",
                    "added_layers": [ { "tar": "3" } ],
                    "layers": [ { "tar": "1" }, { "tar": "2" } ]
                }"#,
            )
            .unwrap_err(),
            "field `added_layers` set before `image` with a `use` of `layers`",
        );
    }

    #[test]
    fn added_layers_after_layers() {
        assert_error(
            parse_job(
                r#"{
                    "program": "/bin/sh",
                    "layers": [ { "tar": "1" }, { "tar": "2" } ],
                    "added_layers": [ { "tar": "3" } ]
                }"#,
            )
            .unwrap_err(),
            "field `added_layers` cannot be set with `layer` field",
        );
    }

    #[test]
    fn added_layers_before_image_with_layers() {
        assert_error(
            parse_job(
                r#"{
                    "program": "/bin/sh",
                    "added_layers": [ "3" ],
                    "image": { "name": "image1", "use": [ "layers" ] }
                }"#,
            )
            .unwrap_err(),
            "field `added_layers` set before `image` with a `use` of `layers`",
        );
    }

    #[test]
    fn arguments() {
        assert_eq!(
            parse_job(
                r#"{
                    "program": "/bin/sh",
                    "layers": [ { "tar": "1" } ],
                    "arguments": [ "-e", "echo foo" ]
                }"#,
            )
            .unwrap()
            .into_job_spec(layer_mapper, images)
            .unwrap(),
            JobSpec::new(
                string!("/bin/sh"),
                nonempty![(digest!(1), ArtifactType::Tar)]
            )
            .arguments(["-e", "echo foo"]),
        )
    }

    #[test]
    fn environment() {
        assert_eq!(
            parse_job(
                r#"{
                    "program": "/bin/sh",
                    "layers": [ { "tar": "1" } ],
                    "environment": { "FOO": "foo", "BAR": "bar" }
                }"#,
            )
            .unwrap()
            .into_job_spec(layer_mapper, images)
            .unwrap(),
            JobSpec::new(
                string!("/bin/sh"),
                nonempty![(digest!(1), ArtifactType::Tar)]
            )
            .environment([("BAR", "bar"), ("FOO", "foo")]),
        )
    }

    #[test]
    fn environment_from_image() {
        assert_eq!(
            parse_job(
                r#"{
                    "program": "/bin/sh",
                    "layers": [ { "tar": "1" } ],
                    "image": { "name": "image1", "use": [ "environment" ] }
                }"#,
            )
            .unwrap()
            .into_job_spec(layer_mapper, images)
            .unwrap(),
            JobSpec::new(
                string!("/bin/sh"),
                nonempty![(digest!(1), ArtifactType::Tar)]
            )
            .environment([("BAZ", "image-baz"), ("FOO", "image-foo")]),
        )
    }

    #[test]
    fn environment_from_image_after_implicit_environment() {
        assert_error(
            parse_job(
                r#"{
                    "program": "/bin/sh",
                    "layers": [ { "tar": "1" } ],
                    "environment": { "FOO": "foo", "BAR": "bar" },
                    "image": { "name": "image1", "use": [ "environment" ] }
                }"#,
            )
            .unwrap_err(),
            "field `image` cannot use `environment` if `environment` has not provided `extend` flags"
        )
    }

    #[test]
    fn environment_from_image_after_explicit_environment() {
        assert_eq!(
            parse_job(
                r#"{
                    "program": "/bin/sh",
                    "layers": [ { "tar": "1" } ],
                    "environment": [ { "vars": { "FOO": "foo", "BAR": "bar" }, "extend": true } ],
                    "image": { "name": "image1", "use": [ "environment" ] }
                }"#,
            )
            .unwrap()
            .into_job_spec(layer_mapper, images)
            .unwrap(),
            JobSpec::new(
                string!("/bin/sh"),
                nonempty![(digest!(1), ArtifactType::Tar)],
            )
            .environment(vec![
                EnvironmentSpec {
                    vars: btreemap! {
                        "BAZ".into() => "image-baz".into(),
                        "FOO".into() => "image-foo".into(),
                    },
                    extend: false,
                },
                EnvironmentSpec {
                    vars: btreemap! {
                        "FOO".into() => "foo".into(),
                        "BAR".into() => "bar".into(),
                    },
                    extend: true,
                }
            ]),
        )
    }

    #[test]
    fn implicit_environment_after_environment_from_image() {
        assert_error(
            parse_job(
                r#"{
                    "program": "/bin/sh",
                    "layers": [ { "tar": "1" } ],
                    "image": { "name": "image1", "use": [ "environment" ] },
                    "environment": { "FOO": "foo", "BAR": "bar" }
                }"#,
            )
            .unwrap_err(),
            concat!(
                "field `environment` must provide `extend` flags if `image` with a `use` of ",
                "`environment` is also set"
            ),
        )
    }

    #[test]
    fn explicit_environment_after_environment_from_image() {
        assert_eq!(
            parse_job(
                r#"{
                    "program": "/bin/sh",
                    "layers": [ { "tar": "1" } ],
                    "image": { "name": "image1", "use": [ "environment" ] },
                    "environment": [ { "vars": { "FOO": "foo", "BAR": "bar" }, "extend": true } ]
                }"#,
            )
            .unwrap()
            .into_job_spec(layer_mapper, images)
            .unwrap(),
            JobSpec::new(
                string!("/bin/sh"),
                nonempty![(digest!(1), ArtifactType::Tar)],
            )
            .environment(vec![
                EnvironmentSpec {
                    vars: btreemap! {
                        "BAZ".into() => "image-baz".into(),
                        "FOO".into() => "image-foo".into(),
                    },
                    extend: false,
                },
                EnvironmentSpec {
                    vars: btreemap! {
                        "FOO".into() => "foo".into(),
                        "BAR".into() => "bar".into(),
                    },
                    extend: true,
                }
            ]),
        )
    }

    #[test]
    fn multi_explicit_environment() {
        assert_eq!(
            parse_job(
                r#"{
                    "program": "/bin/sh",
                    "layers": [ { "tar": "1" } ],
                    "image": { "name": "image1", "use": [ "environment" ] },
                    "environment": [
                        { "vars": { "FOO": "foo", "BAR": "bar" }, "extend": true },
                        { "vars": { "BAZ": "baz", "QUX": "qux" }, "extend": false }
                    ]
                }"#,
            )
            .unwrap()
            .into_job_spec(layer_mapper, images)
            .unwrap(),
            JobSpec::new(
                string!("/bin/sh"),
                nonempty![(digest!(1), ArtifactType::Tar)],
            )
            .environment(vec![
                EnvironmentSpec {
                    vars: btreemap! {
                        "BAZ".into() => "image-baz".into(),
                        "FOO".into() => "image-foo".into(),
                    },
                    extend: false,
                },
                EnvironmentSpec {
                    vars: btreemap! {
                        "FOO".into() => "foo".into(),
                        "BAR".into() => "bar".into(),
                    },
                    extend: true,
                },
                EnvironmentSpec {
                    vars: btreemap! {
                        "BAZ".into() => "baz".into(),
                        "QUX".into() => "qux".into(),
                    },
                    extend: false,
                },
            ]),
        )
    }

    #[test]
    fn devices() {
        assert_eq!(
            parse_job(
                r#"{
                    "program": "/bin/sh",
                    "layers": [ { "tar": "1" } ],
                    "devices": [ "null", "zero" ]
                }"#,
            )
            .unwrap()
            .into_job_spec(layer_mapper, images)
            .unwrap(),
            JobSpec::new(
                string!("/bin/sh"),
                nonempty![(digest!(1), ArtifactType::Tar)]
            )
            .devices(enum_set! {JobDevice::Null | JobDevice::Zero}),
        )
    }

    #[test]
    fn mounts() {
        assert_eq!(
            parse_job(
                r#"{
                    "program": "/bin/sh",
                    "layers": [ { "tar": "1" } ],
                    "mounts": [
                        { "fs_type": "tmp", "mount_point": "/tmp" }
                    ]
                }"#,
            )
            .unwrap()
            .into_job_spec(layer_mapper, images)
            .unwrap(),
            JobSpec::new(
                string!("/bin/sh"),
                nonempty![(digest!(1), ArtifactType::Tar)]
            )
            .mounts([JobMount {
                fs_type: JobMountFsType::Tmp,
                mount_point: utf8_path_buf!("/tmp"),
            }])
        )
    }

    #[test]
    fn enable_loopback() {
        assert_eq!(
            parse_job(
                r#"{
                    "program": "/bin/sh",
                    "layers": [ { "tar": "1" } ],
                    "enable_loopback": true
                }"#,
            )
            .unwrap()
            .into_job_spec(layer_mapper, images)
            .unwrap(),
            JobSpec::new(
                string!("/bin/sh"),
                nonempty![(digest!(1), ArtifactType::Tar)]
            )
            .enable_loopback(true),
        )
    }

    #[test]
    fn enable_writable_file_system() {
        assert_eq!(
            parse_job(
                r#"{
                    "program": "/bin/sh",
                    "layers": [ { "tar": "1" } ],
                    "enable_writable_file_system": true
                }"#,
            )
            .unwrap()
            .into_job_spec(layer_mapper, images)
            .unwrap(),
            JobSpec::new(
                string!("/bin/sh"),
                nonempty![(digest!(1), ArtifactType::Tar)]
            )
            .enable_writable_file_system(true),
        )
    }

    #[test]
    fn working_directory() {
        assert_eq!(
            parse_job(
                r#"{
                    "program": "/bin/sh",
                    "layers": [ { "tar": "1" } ],
                    "working_directory": "/foo/bar"
                }"#,
            )
            .unwrap()
            .into_job_spec(layer_mapper, images)
            .unwrap(),
            JobSpec::new(
                string!("/bin/sh"),
                nonempty![(digest!(1), ArtifactType::Tar)]
            )
            .working_directory("/foo/bar"),
        )
    }

    #[test]
    fn working_directory_from_image() {
        assert_eq!(
            parse_job(
                r#"{
                    "program": "/bin/sh",
                    "layers": [ { "tar": "1" } ],
                    "image": {
                        "name": "image1",
                        "use": [ "working_directory" ]
                    }
                }"#,
            )
            .unwrap()
            .into_job_spec(layer_mapper, images)
            .unwrap(),
            JobSpec::new(
                string!("/bin/sh"),
                nonempty![(digest!(1), ArtifactType::Tar)]
            )
            .working_directory("/foo"),
        )
    }

    #[test]
    fn working_directory_from_image_after_working_directory() {
        assert_error(
            parse_job(
                r#"{
                    "program": "/bin/sh",
                    "layers": [ { "tar": "1" } ],
                    "working_directory": "/foo/bar",
                    "image": {
                        "name": "image1",
                        "use": [ "working_directory" ]
                    }
                }"#,
            )
            .unwrap_err(),
            "field `image` cannot use `working_directory` if field `working_directory` is also set",
        )
    }

    #[test]
    fn working_directory_after_working_directory_from_image() {
        assert_error(
            parse_job(
                r#"{
                    "program": "/bin/sh",
                    "layers": [ { "tar": "1" } ],
                    "image": {
                        "name": "image1",
                        "use": [ "working_directory" ]
                    },
                    "working_directory": "/foo/bar"
                }"#,
            )
            .unwrap_err(),
            concat!(
                "field `working_directory` cannot be set if `image` with a `use` of ",
                "`working_directory` is also set",
            ),
        )
    }

    #[test]
    fn user() {
        assert_eq!(
            parse_job(
                r#"{
                    "program": "/bin/sh",
                    "layers": [ { "tar": "1" } ],
                    "user": 1234
                }"#,
            )
            .unwrap()
            .into_job_spec(layer_mapper, images)
            .unwrap(),
            JobSpec::new(
                string!("/bin/sh"),
                nonempty![(digest!(1), ArtifactType::Tar)]
            )
            .user(1234),
        )
    }

    #[test]
    fn group() {
        assert_eq!(
            parse_job(
                r#"{
                    "program": "/bin/sh",
                    "layers": [ { "tar": "1" } ],
                    "group": 4321
                }"#,
            )
            .unwrap()
            .into_job_spec(layer_mapper, images)
            .unwrap(),
            JobSpec::new(
                string!("/bin/sh"),
                nonempty![(digest!(1), ArtifactType::Tar)]
            )
            .group(4321),
        )
    }

    #[test]
    fn timeout() {
        assert_eq!(
            parse_job(
                r#"{
                    "program": "/bin/sh",
                    "layers": [ { "tar": "1" } ],
                    "timeout": 1234
                }"#,
            )
            .unwrap()
            .into_job_spec(layer_mapper, images)
            .unwrap(),
            JobSpec::new(
                string!("/bin/sh"),
                nonempty![(digest!(1), ArtifactType::Tar)]
            )
            .timeout(Timeout::new(1234)),
        )
    }

    #[test]
    fn timeout_0() {
        assert_eq!(
            parse_job(
                r#"{
                    "program": "/bin/sh",
                    "layers": [ { "tar": "1" } ],
                    "timeout": 0
                }"#,
            )
            .unwrap()
            .into_job_spec(layer_mapper, images)
            .unwrap(),
            JobSpec::new(
                string!("/bin/sh"),
                nonempty![(digest!(1), ArtifactType::Tar)]
            )
            .timeout(Timeout::new(0)),
        )
    }
}
