pub mod substitute;

use anyhow::{anyhow, Error, Result};
use enumset::{EnumSet, EnumSetType};
use serde::{de, Deserialize, Serialize};
use std::{
    collections::BTreeMap,
    env::{self, VarError},
    path::PathBuf,
};
use tuple::Map as _;

pub fn std_env_lookup(var: &str) -> Result<Option<String>> {
    match env::var(var) {
        Ok(val) => Ok(Some(val)),
        Err(VarError::NotPresent) => Ok(None),
        Err(err) => Err(Error::new(err)),
    }
}

#[derive(Default)]
pub struct ImageConfig {
    pub layers: Vec<PathBuf>,
    pub working_directory: Option<PathBuf>,
    pub environment: Option<Vec<String>>,
}

#[derive(Debug, Deserialize, EnumSetType, Serialize)]
#[serde(rename_all = "snake_case")]
#[enumset(serialize_repr = "list")]
pub enum ImageUse {
    Layers,
    Environment,
    WorkingDirectory,
}

#[derive(Deserialize)]
pub struct Image {
    pub name: String,
    #[serde(rename = "use")]
    pub use_: EnumSet<ImageUse>,
}

#[derive(PartialEq, Eq, Debug, Deserialize)]
pub enum PossiblyImage<T> {
    Image,
    Explicit(T),
}

pub struct ImageOption<'a> {
    name: Option<&'a str>,
    layers: Vec<PathBuf>,
    environment: Option<Vec<String>>,
    working_directory: Option<PathBuf>,
}

impl<'a> ImageOption<'a> {
    pub fn new(
        image_name: &'a Option<String>,
        image_lookup: impl FnMut(&str) -> Result<ImageConfig>,
    ) -> Result<Self> {
        let name = image_name.as_deref();
        let (layers, environment, working_directory) =
            image_name.as_deref().map(image_lookup).transpose()?.map_or(
                (Default::default(), Default::default(), Default::default()),
                |ImageConfig {
                     layers,
                     environment,
                     working_directory,
                 }| { (layers, environment, working_directory) },
            );
        Ok(ImageOption {
            name,
            layers,
            environment,
            working_directory,
        })
    }

    pub fn name(&self) -> &str {
        self.name.unwrap()
    }

    pub fn layers(&self) -> Result<impl Iterator<Item = String>> {
        Ok(self
            .layers
            .iter()
            .map(|p| {
                p.as_os_str().to_str().map(str::to_string).ok_or_else(|| {
                    anyhow!("image {} has a non-UTF-8 layer path {p:?}", self.name())
                })
            })
            .collect::<Result<Vec<_>>>()?
            .into_iter())
    }

    pub fn environment(&self) -> Result<BTreeMap<String, String>> {
        Ok(BTreeMap::from_iter(
            self.environment
                .as_ref()
                .ok_or_else(|| anyhow!("image {} has no environment to use", self.name()))?
                .iter()
                .map(|var| {
                    var.split_once('=')
                        .map(|pair| pair.map(str::to_string))
                        .ok_or_else(|| {
                            anyhow!(
                                "image {} has an invalid environment variable {var}",
                                self.name(),
                            )
                        })
                })
                .collect::<Result<Vec<_>>>()?,
        ))
    }

    pub fn working_directory(&self) -> Result<PathBuf> {
        self.working_directory
            .as_ref()
            .map(PathBuf::clone)
            .ok_or_else(|| anyhow!("image {} has no working directory to use", self.name()))
    }
}

pub fn incompatible<T, E>(field: &Option<T>, msg: &str) -> std::result::Result<(), E>
where
    E: de::Error,
{
    if field.is_some() {
        Err(E::custom(format_args!("{}", msg)))
    } else {
        Ok(())
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use meticulous_test::{path_buf_vec, string, string_vec};
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
        env::set_var(var, &val);
        assert_eq!(
            format!("{}", std_env_lookup(var).unwrap_err()),
            r#"environment variable was not valid unicode: "\xFF""#
        );
    }

    fn images(name: &str) -> Result<ImageConfig> {
        match name {
            "image1" => Ok(ImageConfig {
                layers: path_buf_vec!["42", "43"],
                working_directory: Some("/foo".into()),
                environment: Some(string_vec!["FOO=image-foo", "BAZ=image-baz",]),
            }),
            "empty" => Ok(Default::default()),
            "invalid-env" => Ok(ImageConfig {
                environment: Some(string_vec!["FOO"]),
                ..Default::default()
            }),
            "invalid-layer-path" => Ok(ImageConfig {
                layers: vec![PathBuf::from(OsStr::from_bytes(b"\xff"))],
                ..Default::default()
            }),
            _ => Err(anyhow!("no container named {name} found")),
        }
    }

    fn assert_error(err: anyhow::Error, expected: &str) {
        let message = format!("{err}");
        assert!(
            message == expected,
            "message: {message:?}, expected: {expected:?}"
        );
    }

    #[test]
    fn good_image_option() {
        let image_name = Some(string!("image1"));
        let io = ImageOption::new(&image_name, images).unwrap();
        assert_eq!(io.name(), "image1");
        assert_eq!(
            Vec::from_iter(io.layers().unwrap()),
            string_vec!["42", "43"],
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
        let image_name = Some(string!("empty"));
        let io = ImageOption::new(&image_name, images).unwrap();
        assert_error(
            io.environment().unwrap_err(),
            "image empty has no environment to use",
        );
        assert_error(
            io.working_directory().unwrap_err(),
            "image empty has no working directory to use",
        );
    }

    #[test]
    fn image_option_invalid_environment_variable() {
        let image_name = Some(string!("invalid-env"));
        let io = ImageOption::new(&image_name, images).unwrap();
        assert_error(
            io.environment().unwrap_err(),
            "image invalid-env has an invalid environment variable FOO",
        );
    }

    #[test]
    fn image_option_invalid_layer_path() {
        let image_name = Some(string!("invalid-layer-path"));
        let io = ImageOption::new(&image_name, images).unwrap();
        let Err(err) = io.layers() else {
            panic!("");
        };
        assert_error(
            err,
            r#"image invalid-layer-path has a non-UTF-8 layer path "\xFF""#,
        );
    }
}
