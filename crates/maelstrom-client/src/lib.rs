pub use maelstrom_client_process::{
    spec, test, ClientDriverMode, JobResponseHandler, MANIFEST_DIR,
};

use anyhow::Result;
use indicatif::ProgressBar;
use maelstrom_base::{stats::JobStateCounts, ArtifactType, JobSpec, Sha256Digest};
use maelstrom_client_process::Client as ProcessClient;
use maelstrom_container::ContainerImage;
use maelstrom_util::config::BrokerAddr;
use spec::Layer;
use std::path::Path;
use std::sync::mpsc::Receiver;

pub struct Client {
    inner: ProcessClient,
}

impl Client {
    pub fn new(
        driver_mode: ClientDriverMode,
        broker_addr: BrokerAddr,
        project_dir: impl AsRef<Path>,
        cache_dir: impl AsRef<Path>,
    ) -> Result<Self> {
        Ok(Self {
            inner: ProcessClient::new(driver_mode, broker_addr, project_dir, cache_dir)?,
        })
    }

    pub fn add_artifact(&mut self, path: &Path) -> Result<Sha256Digest> {
        self.inner.add_artifact(path)
    }

    pub fn add_layer(&mut self, layer: Layer) -> Result<(Sha256Digest, ArtifactType)> {
        self.inner.add_layer(layer)
    }

    pub fn get_container_image(
        &mut self,
        name: &str,
        tag: &str,
        prog: ProgressBar,
    ) -> Result<ContainerImage> {
        self.inner.get_container_image(name, tag, prog)
    }

    pub fn add_job(&mut self, spec: JobSpec, handler: JobResponseHandler) {
        self.inner.add_job(spec, handler)
    }

    pub fn stop_accepting(&mut self) -> Result<()> {
        self.inner.stop_accepting()
    }

    pub fn wait_for_outstanding_jobs(&mut self) -> Result<()> {
        self.inner.wait_for_outstanding_jobs()
    }

    pub fn get_job_state_counts(&mut self) -> Result<Receiver<JobStateCounts>> {
        self.inner.get_job_state_counts()
    }

    /// Must only be called if created with `ClientDriverMode::SingleThreaded`
    pub fn process_broker_msg_single_threaded(&self, count: usize) {
        self.inner.process_broker_msg_single_threaded(count)
    }

    /// Must only be called if created with `ClientDriverMode::SingleThreaded`
    pub fn process_client_messages_single_threaded(&self) {
        self.inner.process_client_messages_single_threaded()
    }

    /// Must only be called if created with `ClientDriverMode::SingleThreaded`
    pub fn process_artifact_single_threaded(&self) {
        self.inner.process_artifact_single_threaded()
    }
}
