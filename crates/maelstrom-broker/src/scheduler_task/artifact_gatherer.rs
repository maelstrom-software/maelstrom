use crate::{cache::SchedulerCache, scheduler_task::ManifestReadRequest};
use anyhow::Result;
use maelstrom_base::{ArtifactType, ClientId, ClientJobId, JobId, NonEmpty, Sha256Digest};
use maelstrom_util::{
    cache::GetArtifact,
    ext::{BoolExt as _, OptionExt as _},
};
use std::{
    collections::{hash_map::Entry, HashMap, HashSet},
    path::PathBuf,
};

pub trait Deps {
    type ArtifactStream;
    fn send_message_to_manifest_reader(
        &mut self,
        request: ManifestReadRequest<Self::ArtifactStream>,
    );
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum IsManifest {
    Manifest,
    NotManifest,
}

impl From<bool> for IsManifest {
    fn from(is_manifest: bool) -> Self {
        if is_manifest {
            Self::Manifest
        } else {
            Self::NotManifest
        }
    }
}

impl IsManifest {
    fn is_manifest(&self) -> bool {
        self == &Self::Manifest
    }
}

#[derive(Default)]
struct Job {
    acquired_artifacts: HashSet<Sha256Digest>,
    missing_artifacts: HashMap<Sha256Digest, IsManifest>,
    manifests_being_read: HashSet<Sha256Digest>,
}

impl Job {
    fn have_all_artifacts(&self) -> bool {
        self.missing_artifacts.is_empty() && self.manifests_being_read.is_empty()
    }
}

#[derive(Default)]
struct Client {
    jobs: HashMap<ClientJobId, Job>,
}

#[must_use]
pub enum StartJob {
    NotReady(Vec<Sha256Digest>),
    Ready,
}

pub struct ArtifactGatherer<CacheT: SchedulerCache, DepsT: Deps> {
    cache: CacheT,
    deps: DepsT,
    clients: HashMap<ClientId, Client>,
}

impl<CacheT, DepsT> ArtifactGatherer<CacheT, DepsT>
where
    CacheT: SchedulerCache,
    DepsT: Deps<ArtifactStream = CacheT::ArtifactStream>,
{
    pub fn new(cache: CacheT, deps: DepsT) -> Self {
        Self {
            deps,
            cache,
            clients: Default::default(),
        }
    }

    fn client_connected(&mut self, cid: ClientId) {
        self.clients
            .insert(cid, Default::default())
            .assert_is_none();
    }

    fn client_disconnected(&mut self, cid: ClientId) {
        self.cache.client_disconnected(cid);

        let client = self.clients.remove(&cid).unwrap();
        for job in client.jobs.into_values() {
            for artifact in job.acquired_artifacts {
                self.cache.decrement_refcount(&artifact);
            }
        }
    }

    #[must_use]
    fn start_artifact_acquisition_for_job(
        cache: &mut CacheT,
        deps: &mut DepsT,
        digest: &Sha256Digest,
        is_manifest: IsManifest,
        jid: JobId,
        job: &mut Job,
    ) -> bool {
        if job.acquired_artifacts.contains(digest) || job.missing_artifacts.contains_key(digest) {
            return false;
        }
        match cache.get_artifact(jid, digest.clone()) {
            GetArtifact::Success => {
                Self::complete_artifact_acquisition_for_job(
                    cache,
                    deps,
                    digest,
                    is_manifest,
                    jid,
                    job,
                );
                false
            }
            GetArtifact::Wait => {
                job.missing_artifacts
                    .insert(digest.clone(), is_manifest)
                    .assert_is_none();
                false
            }
            GetArtifact::Get => {
                job.missing_artifacts
                    .insert(digest.clone(), is_manifest)
                    .assert_is_none();
                true
            }
        }
    }

    fn complete_artifact_acquisition_for_job(
        cache: &mut CacheT,
        deps: &mut DepsT,
        digest: &Sha256Digest,
        is_manifest: IsManifest,
        jid: JobId,
        job: &mut Job,
    ) {
        job.acquired_artifacts
            .insert(digest.clone())
            .assert_is_true();
        if is_manifest.is_manifest() {
            let manifest_stream = cache.read_artifact(digest);
            deps.send_message_to_manifest_reader(ManifestReadRequest {
                manifest_stream,
                digest: digest.clone(),
                jid,
            });
            job.manifests_being_read
                .insert(digest.clone())
                .assert_is_true();
        }
    }

    fn start_job(
        &mut self,
        jid: JobId,
        layers: NonEmpty<(Sha256Digest, ArtifactType)>,
    ) -> StartJob {
        let JobId { cid, cjid } = jid;
        let client = self.clients.get_mut(&cid).unwrap();
        let Entry::Vacant(job_entry) = client.jobs.entry(cjid) else {
            panic!("job entry already exists for {jid:?}");
        };
        let job = job_entry.insert(Default::default());

        let mut artifacts_to_fetch = vec![];
        for (digest, type_) in layers {
            let is_manifest = IsManifest::from(type_ == ArtifactType::Manifest);
            let fetch_artifact = Self::start_artifact_acquisition_for_job(
                &mut self.cache,
                &mut self.deps,
                &digest,
                is_manifest,
                jid,
                job,
            );
            if fetch_artifact {
                artifacts_to_fetch.push(digest);
            }
        }

        if job.have_all_artifacts() {
            StartJob::Ready
        } else {
            StartJob::NotReady(artifacts_to_fetch)
        }
    }

    fn artifact_transferred(
        &mut self,
        digest: &Sha256Digest,
        file: Option<CacheT::TempFile>,
    ) -> Result<HashSet<JobId>> {
        Ok(self
            .cache
            .got_artifact(digest, file)?
            .into_iter()
            .filter(|jid| {
                let client = self.clients.get_mut(&jid.cid).unwrap();
                let job = client.jobs.get_mut(&jid.cjid).unwrap();
                let is_manifest = job.missing_artifacts.remove(digest).unwrap();
                Self::complete_artifact_acquisition_for_job(
                    &mut self.cache,
                    &mut self.deps,
                    digest,
                    is_manifest,
                    *jid,
                    job,
                );
                job.have_all_artifacts()
            })
            .collect())
    }

    fn manifest_read_for_job_entry(&mut self, digest: &Sha256Digest, jid: JobId) -> bool {
        let Some(client) = self.clients.get_mut(&jid.cid) else {
            // This indicates that the client isn't around anymore. Just ignore this message. When
            // the client disconnected, we canceled all of the outstanding requests. Ideally, we
            // would have a way to cancel the outstanding manifest read, but we don't currently
            // have that.
            return false;
        };
        let job = client.jobs.get_mut(&jid.cjid).unwrap();
        Self::start_artifact_acquisition_for_job(
            &mut self.cache,
            &mut self.deps,
            digest,
            IsManifest::NotManifest,
            jid,
            job,
        )
    }

    #[must_use]
    fn manifest_read_for_job_complete(
        &mut self,
        digest: Sha256Digest,
        jid: JobId,
        result: anyhow::Result<()>,
    ) -> bool {
        // It would be better to not crash...
        result.expect("failed reading a manifest");

        let Some(client) = self.clients.get_mut(&jid.cid) else {
            // This indicates that the client isn't around anymore. Just ignore this message. When
            // the client disconnected, we canceled all of the outstanding requests.
            return false;
        };
        let job = client.jobs.get_mut(&jid.cjid).unwrap();
        job.manifests_being_read.remove(&digest).assert_is_true();

        job.have_all_artifacts()
    }

    fn complete_job(&mut self, jid: JobId) {
        let client = self.clients.get_mut(&jid.cid).unwrap();
        let job = client.jobs.remove(&jid.cjid).unwrap();
        for artifact in job.acquired_artifacts {
            self.cache.decrement_refcount(&artifact);
        }
    }

    fn get_artifact_for_worker(&mut self, digest: &Sha256Digest) -> Option<(PathBuf, u64)> {
        self.cache.get_artifact_for_worker(digest)
    }

    fn receive_decrement_refcount(&mut self, digest: Sha256Digest) {
        self.cache.decrement_refcount(&digest);
    }

    fn get_waiting_for_artifacts_count(&self, cid: ClientId) -> u64 {
        self.clients
            .get(&cid)
            .unwrap()
            .jobs
            .values()
            .filter(|job| !job.have_all_artifacts())
            .count() as u64
    }
}

impl<CacheT, DepsT> super::scheduler::ArtifactGatherer for ArtifactGatherer<CacheT, DepsT>
where
    CacheT: SchedulerCache,
    DepsT: Deps<ArtifactStream = CacheT::ArtifactStream>,
{
    type TempFile = CacheT::TempFile;

    fn client_connected(&mut self, cid: ClientId) {
        self.client_connected(cid)
    }

    fn client_disconnected(&mut self, cid: ClientId) {
        self.client_disconnected(cid)
    }

    fn start_job(
        &mut self,
        jid: JobId,
        layers: NonEmpty<(Sha256Digest, ArtifactType)>,
    ) -> StartJob {
        self.start_job(jid, layers)
    }

    fn artifact_transferred(
        &mut self,
        digest: &Sha256Digest,
        file: Option<Self::TempFile>,
    ) -> Result<HashSet<JobId>> {
        self.artifact_transferred(digest, file)
    }

    #[must_use]
    fn manifest_read_for_job_entry(&mut self, digest: &Sha256Digest, jid: JobId) -> bool {
        self.manifest_read_for_job_entry(digest, jid)
    }

    #[must_use]
    fn manifest_read_for_job_complete(
        &mut self,
        digest: Sha256Digest,
        jid: JobId,
        result: anyhow::Result<()>,
    ) -> bool {
        self.manifest_read_for_job_complete(digest, jid, result)
    }

    fn complete_job(&mut self, jid: JobId) {
        self.complete_job(jid)
    }

    fn get_artifact_for_worker(&mut self, digest: &Sha256Digest) -> Option<(PathBuf, u64)> {
        self.get_artifact_for_worker(digest)
    }

    fn receive_decrement_refcount(&mut self, digest: Sha256Digest) {
        self.receive_decrement_refcount(digest)
    }

    fn get_waiting_for_artifacts_count(&self, cid: ClientId) -> u64 {
        self.get_waiting_for_artifacts_count(cid)
    }
}
