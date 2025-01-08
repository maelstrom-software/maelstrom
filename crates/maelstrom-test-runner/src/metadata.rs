mod directive;
pub mod store;

pub use store::Store;

use anyhow::Result;
use directive::{Directive, DirectiveContainer};
use maelstrom_base::Timeout;
use maelstrom_client::spec::{ContainerSpec, EnvironmentSpec};

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct Metadata {
    pub container: ContainerSpec,
    include_shared_libraries: Option<bool>,
    uses_image_layers: Option<bool>,
    pub timeout: Option<Timeout>,
    pub ignore: bool,
}

impl Metadata {
    /// Return whether to include a layer of shared library dependencies.
    ///
    /// The logic here is that if they explicitly set the value to something, we should return
    /// that. Otherwise, we should see if they are using layers from an image. If they are, we can
    /// assume that the image has shared libraries, and we shouldn't push shared libraries on top
    /// of it. Otherwise, they probably don't want to have to explicitly provide a layer with
    /// shared libraries in it, so we should include shared libraries for them.
    pub fn include_shared_libraries(&self) -> bool {
        match self.include_shared_libraries {
            Some(val) => val,
            None => !self.uses_image_layers.unwrap(),
        }
    }

    fn try_fold<TestFilterT>(mut self, directive: &Directive<TestFilterT>) -> Result<Self> {
        let rhs = directive;

        self.container = match &rhs.container {
            DirectiveContainer::Override(container) => container.clone(),
            DirectiveContainer::Accumulate(rhs) => {
                let mut layers = rhs.layers.clone().unwrap_or(self.container.layers);
                layers.extend(rhs.added_layers.iter().flatten().cloned());

                let mut environment = self.container.environment;
                if let Some(vars) = &rhs.environment {
                    environment.push(EnvironmentSpec {
                        vars: vars.clone(),
                        extend: false,
                    });
                }
                environment.extend(
                    rhs.added_environment
                        .iter()
                        .cloned()
                        .map(|vars| EnvironmentSpec { vars, extend: true }),
                );

                let working_directory = rhs
                    .working_directory
                    .clone()
                    .or(self.container.working_directory);

                let enable_writable_file_system = rhs
                    .enable_writable_file_system
                    .or(self.container.enable_writable_file_system);

                let mut mounts = rhs.mounts.clone().unwrap_or(self.container.mounts);
                mounts.extend(rhs.added_mounts.iter().flatten().cloned());

                let network = rhs.network.or(self.container.network);

                let user = rhs.user.or(self.container.user);

                let group = rhs.group.or(self.container.group);

                ContainerSpec {
                    parent: self.container.parent,
                    layers,
                    environment,
                    working_directory,
                    enable_writable_file_system,
                    mounts,
                    network,
                    user,
                    group,
                }
            }
        };

        self.include_shared_libraries = directive
            .include_shared_libraries
            .or(self.include_shared_libraries);
        self.timeout = directive.timeout.unwrap_or(self.timeout);
        self.ignore = directive.ignore.unwrap_or(self.ignore);

        Ok(self)
    }
}
