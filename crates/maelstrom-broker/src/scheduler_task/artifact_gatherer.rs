use crate::cache::SchedulerCache;
use get_size::GetSize;
use maelstrom_base::{
    ArtifactType, ArtifactUploadLocation, ClientId, ClientJobId, JobId, NonEmpty, Sha256Digest,
};
use maelstrom_util::{
    cache::GetArtifact,
    ext::{BoolExt as _, OptionExt as _},
    heap::{Heap, HeapDeps, HeapIndex},
};
use std::{
    collections::{hash_map::Entry, HashMap, HashSet, VecDeque},
    num::NonZeroUsize,
    ops::{Deref, DerefMut},
    path::PathBuf,
};

pub trait Deps {
    type ArtifactStream;
    type WorkerArtifactFetcherSender;
    type ClientSender;
    fn send_read_request_to_manifest_reader(
        &mut self,
        manifest_stream: Self::ArtifactStream,
        manifest_digest: Sha256Digest,
    );
    fn send_response_to_worker_artifact_fetcher(
        &mut self,
        sender: &mut Self::WorkerArtifactFetcherSender,
        message: Option<(PathBuf, u64)>,
    );
    fn send_transfer_artifact_to_client(
        &mut self,
        sender: &mut Self::ClientSender,
        digest: Sha256Digest,
    );
    fn send_general_error_to_client(&mut self, sender: &mut Self::ClientSender, error: String);
    fn send_jobs_ready_to_scheduler(&mut self, jobs: NonEmpty<JobId>);
    fn send_jobs_failed_to_scheduler(&mut self, jobs: NonEmpty<JobId>, err: String);
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum IsManifest<T> {
    NotManifest,
    Manifest(T),
}

impl<T> IsManifest<T> {
    fn new(is_manifest: bool, t: T) -> Self {
        if is_manifest {
            Self::Manifest(t)
        } else {
            Self::NotManifest
        }
    }

    fn map<U>(self, f: impl FnOnce(T) -> U) -> IsManifest<U> {
        match self {
            Self::NotManifest => IsManifest::NotManifest,
            Self::Manifest(t) => IsManifest::Manifest(f(t)),
        }
    }
}

#[derive(Default)]
struct Job {
    artifacts_acquired: HashSet<Sha256Digest>,
    artifacts_being_acquired: HashMap<Sha256Digest, IsManifest<()>>,
    manifests_being_read: HashSet<Sha256Digest>,
}

impl Job {
    fn have_all_artifacts(&self) -> bool {
        self.artifacts_being_acquired.is_empty() && self.manifests_being_read.is_empty()
    }
}

struct Client<DepsT: Deps> {
    sender: DepsT::ClientSender,
    jobs: HashMap<ClientJobId, Job>,
}

impl<DepsT: Deps> Client<DepsT> {
    fn new(sender: DepsT::ClientSender) -> Self {
        Self {
            sender,
            jobs: Default::default(),
        }
    }
}

#[must_use]
#[derive(Debug, PartialEq)]
pub enum StartJob {
    Ready,
    NotReady,
}

struct ManifestReadEntry {
    jobs: HashSet<JobId>,
    entries: HashSet<Sha256Digest>,
}

#[derive(GetSize)]
struct ManifestReadCacheEntry {
    entries: HashSet<Sha256Digest>,
    heap_index: HeapIndex,
    last_read: u64,
    size: usize,
}

#[derive(Default)]
struct ManifestReadCacheMap(HashMap<Sha256Digest, ManifestReadCacheEntry>);

impl HeapDeps for ManifestReadCacheMap {
    type Element = Sha256Digest;

    fn is_element_less_than(&self, lhs: &Self::Element, rhs: &Self::Element) -> bool {
        let lhs = self.0.get(lhs).unwrap();
        let rhs = self.0.get(rhs).unwrap();
        lhs.last_read < rhs.last_read
    }

    fn update_index(&mut self, elem: &Self::Element, heap_index: HeapIndex) {
        self.0.get_mut(elem).unwrap().heap_index = heap_index;
    }
}

impl Deref for ManifestReadCacheMap {
    type Target = HashMap<Sha256Digest, ManifestReadCacheEntry>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl DerefMut for ManifestReadCacheMap {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

struct ManifestReads {
    entries: HashMap<Sha256Digest, ManifestReadEntry>,
    cache: ManifestReadCacheMap,
    cache_heap: Heap<ManifestReadCacheMap>,
    waiting: VecDeque<Sha256Digest>,
    in_progress: usize,
    max_in_progress: NonZeroUsize,
    next_read: u64,
    cache_size: usize,
    max_cache_size: usize,
}

pub struct ArtifactGatherer<CacheT: SchedulerCache, DepsT: Deps> {
    cache: CacheT,
    deps: DepsT,
    clients: HashMap<ClientId, Client<DepsT>>,
    tcp_upload_landing_pad: HashMap<Sha256Digest, CacheT::TempFile>,
    manifest_reads: ManifestReads,
}

impl<CacheT, DepsT> ArtifactGatherer<CacheT, DepsT>
where
    CacheT: SchedulerCache,
    DepsT: Deps<ArtifactStream = CacheT::ArtifactStream>,
{
    pub fn new(
        cache: CacheT,
        deps: DepsT,
        max_cache_size: usize,
        max_simultaneous_manifest_reads: NonZeroUsize,
    ) -> Self {
        Self {
            deps,
            cache,
            clients: Default::default(),
            tcp_upload_landing_pad: Default::default(),
            manifest_reads: ManifestReads {
                entries: Default::default(),
                cache: Default::default(),
                cache_heap: Default::default(),
                waiting: Default::default(),
                in_progress: 0,
                max_in_progress: max_simultaneous_manifest_reads,
                next_read: 0,
                cache_size: 0,
                max_cache_size,
            },
        }
    }

    /// Attempt to look up a job an remove it from its client. This function will panic if the
    /// client doesn't exist. However, if the job doesn't exit, it will return `None`.
    fn try_pop_job(&mut self, jid: JobId) -> Option<Job> {
        self.clients
            .get_mut(&jid.cid)
            .unwrap()
            .jobs
            .remove(&jid.cjid)
    }

    /// Destroy a job, dropping any references it has and removing its `JobId` from
    /// `manifests_being_read`. The `JobId` isn't removed from the cache, as the cache API doesn't
    /// currently support that: it only suppors removing whole client connections.
    fn drop_job(&mut self, jid: JobId, job: Job) {
        for artifact in job.artifacts_acquired {
            self.cache.decrement_refcount(&artifact);
        }
        for artifact in job.manifests_being_read {
            self.manifest_reads
                .entries
                .get_mut(&artifact)
                .unwrap()
                .jobs
                .remove(&jid)
                .assert_is_true();
        }
    }

    /// Accept a client sender for a newly-connected client.
    pub fn client_connected(&mut self, cid: ClientId, sender: DepsT::ClientSender) {
        self.clients
            .insert(cid, Client::new(sender))
            .assert_is_none();
    }

    /// Deal with a client being disconnected. Telling the cache the client disconnected guarantees
    /// that the cache will no longer mention any `JobId`s from that client.
    pub fn client_disconnected(&mut self, cid: ClientId) {
        self.cache.client_disconnected(cid);

        let client = self.clients.remove(&cid).unwrap();
        for (cjid, job) in client.jobs {
            self.drop_job(JobId { cid, cjid }, job);
        }

        for entry in self.manifest_reads.entries.values_mut() {
            entry.jobs.retain(|jid| jid.cid != cid);
        }
    }

    /// Attempt to start a job. If the job can be started immediately, then [`StartJob::Ready`] is
    /// returned, and the references for the job's artifacts are held by the [`ArtifactGatherer`]
    /// on behalf of the job. When the job is done, [`Self::job_completed`] must be called to
    /// release the references.
    ///
    /// If the job can't immediately be started, then [`StartJob::NotReady`] will be returned. The
    /// [`ArtifactGatherer`] will then work with the cache and the manifest reader to determine all
    /// of the artifacts the job needs and to read them into cache and acquire references on them.
    /// When it is done, it will send a message back to the scheduler telling it that the job is
    /// ready to run, or that there was an error and the job can't be run. These two messages are
    /// [`crate::scheduler_task::scheduler::Message::JobsReadyFromArtifactGatherer`] and
    /// [`crate::scheduler_task::scheduler::Message::JobsFailedFromArtifactGatherer`] respectively.
    ///
    /// In the case of the former, [`Self::job_completed`] must be called after the job has been
    /// run to releae the references. In the case the lattter, [`Self::job_completed`] **must not**
    /// be called, as the [`ArtifactGatherer`] will have already released any references the job
    /// may have had, and will have erased any trace of the job from its state.
    pub fn start_job(
        &mut self,
        jid: JobId,
        layers: NonEmpty<(Sha256Digest, ArtifactType)>,
    ) -> StartJob {
        let client = self.clients.get_mut(&jid.cid).unwrap();
        let Entry::Vacant(job_entry) = client.jobs.entry(jid.cjid) else {
            panic!("job entry already exists for {jid}");
        };
        let job = job_entry.insert(Default::default());

        for (digest, type_) in layers {
            Self::start_acquiring_artifact_for_job(
                &mut self.cache,
                &mut client.sender,
                &mut self.deps,
                digest,
                IsManifest::new(type_ == ArtifactType::Manifest, &mut self.manifest_reads),
                jid,
                job,
            );
        }

        if job.have_all_artifacts() {
            StartJob::Ready
        } else {
            StartJob::NotReady
        }
    }

    /// Deal with a potentially new artifact dependency for a job.
    ///
    /// It's possible for a job to depend on a single artifact multiple different ways, so we must
    /// deal with "duplicate" artifacts.
    ///
    /// When we receive a new artifact, we must first make sure it's in cache and that we have a
    /// reference on it. Additionally, if it's a manifest, we must read the manifest and extract
    /// all artifacts it depends on, and then acquire references on them as well.
    fn start_acquiring_artifact_for_job(
        cache: &mut CacheT,
        client_sender: &mut DepsT::ClientSender,
        deps: &mut DepsT,
        digest: Sha256Digest,
        is_manifest: IsManifest<&mut ManifestReads>,
        jid: JobId,
        job: &mut Job,
    ) {
        if job.artifacts_acquired.contains(&digest)
            || job.artifacts_being_acquired.contains_key(&digest)
        {
            return;
        }
        match cache.get_artifact(jid, digest.clone()) {
            GetArtifact::Success => {
                job.artifacts_acquired
                    .insert(digest.clone())
                    .assert_is_true();
                Self::potentially_start_reading_manifest_for_job(
                    cache,
                    client_sender,
                    deps,
                    &digest,
                    is_manifest,
                    jid,
                    job,
                );
            }
            GetArtifact::Wait => {
                job.artifacts_being_acquired
                    .insert(digest, is_manifest.map(drop))
                    .assert_is_none();
            }
            GetArtifact::Get => {
                job.artifacts_being_acquired
                    .insert(digest.clone(), is_manifest.map(drop))
                    .assert_is_none();
                deps.send_transfer_artifact_to_client(client_sender, digest);
            }
        }
    }

    fn start_acquiring_manifest_entries_for_job<'a>(
        cache: &mut CacheT,
        client_sender: &mut DepsT::ClientSender,
        deps: &mut DepsT,
        digests: impl IntoIterator<Item = &'a Sha256Digest>,
        jid: JobId,
        job: &mut Job,
    ) {
        for digest in digests.into_iter() {
            Self::start_acquiring_artifact_for_job(
                cache,
                client_sender,
                deps,
                digest.clone(),
                IsManifest::NotManifest,
                jid,
                job,
            );
        }
    }

    /// Given a newly-acquired artifact, check to see if it's a manifest, and if it is, start
    /// reading the manifest to incorporate the manifests dependencies as well.
    fn potentially_start_reading_manifest_for_job(
        cache: &mut CacheT,
        client_sender: &mut DepsT::ClientSender,
        deps: &mut DepsT,
        digest: &Sha256Digest,
        is_manifest: IsManifest<&mut ManifestReads>,
        jid: JobId,
        job: &mut Job,
    ) {
        let IsManifest::Manifest(manifest_reads) = is_manifest else {
            return;
        };

        if Self::get_manifest_from_cache(
            cache,
            client_sender,
            deps,
            digest,
            jid,
            job,
            manifest_reads,
        ) {
            return;
        }

        job.manifests_being_read
            .insert(digest.clone())
            .assert_is_true();

        match manifest_reads.entries.entry(digest.clone()) {
            Entry::Occupied(entry) => {
                let entry = entry.into_mut();
                entry.jobs.insert(jid).assert_is_true();
                Self::start_acquiring_manifest_entries_for_job(
                    cache,
                    client_sender,
                    deps,
                    &entry.entries,
                    jid,
                    job,
                );
            }
            Entry::Vacant(entry) => {
                entry.insert(ManifestReadEntry {
                    jobs: HashSet::from_iter([jid]),
                    entries: Default::default(),
                });
                Self::start_or_enqueue_manifest_read(cache, deps, digest, manifest_reads);
            }
        }
    }

    #[must_use]
    fn get_manifest_from_cache(
        cache: &mut CacheT,
        client_sender: &mut DepsT::ClientSender,
        deps: &mut DepsT,
        digest: &Sha256Digest,
        jid: JobId,
        job: &mut Job,
        manifest_reads: &mut ManifestReads,
    ) -> bool {
        let Some(entry) = manifest_reads.cache.get_mut(digest) else {
            return false;
        };

        entry.last_read = manifest_reads.next_read;
        Self::start_acquiring_manifest_entries_for_job(
            cache,
            client_sender,
            deps,
            &entry.entries,
            jid,
            job,
        );
        manifest_reads.next_read += 1;
        let heap_index = entry.heap_index;
        manifest_reads
            .cache_heap
            .sift_down(&mut manifest_reads.cache, heap_index);

        true
    }

    fn insert_manifest_into_cache(&mut self, digest: Sha256Digest, entries: HashSet<Sha256Digest>) {
        let mut entry = ManifestReadCacheEntry {
            entries,
            heap_index: Default::default(),
            last_read: self.manifest_reads.next_read,
            size: 0,
        };
        entry.size = entry.get_size();
        self.manifest_reads.next_read += 1;
        self.manifest_reads.cache_size += entry.size;
        self.manifest_reads
            .cache
            .insert(digest.clone(), entry)
            .assert_is_none();
        self.manifest_reads
            .cache_heap
            .push(&mut self.manifest_reads.cache, digest);
        while self.manifest_reads.cache_size > self.manifest_reads.max_cache_size {
            let digest = self
                .manifest_reads
                .cache_heap
                .pop(&mut self.manifest_reads.cache)
                .unwrap();
            let entry = self.manifest_reads.cache.remove(&digest).unwrap();
            self.manifest_reads.cache_size -= entry.size;
        }
    }

    fn start_or_enqueue_manifest_read(
        cache: &mut CacheT,
        deps: &mut DepsT,
        digest: &Sha256Digest,
        manifest_reads: &mut ManifestReads,
    ) {
        if manifest_reads.in_progress == manifest_reads.max_in_progress.get() {
            manifest_reads.waiting.push_back(digest.clone());
        } else {
            Self::start_manifest_reader(cache, deps, digest.clone(), manifest_reads);
        }
    }

    /// Start up a manifest reader, keeping track of how many are pending.
    fn start_manifest_reader(
        cache: &mut CacheT,
        deps: &mut DepsT,
        digest: Sha256Digest,
        manifest_reads: &mut ManifestReads,
    ) {
        assert!(manifest_reads.in_progress < manifest_reads.max_in_progress.get());
        manifest_reads.in_progress += 1;
        let manifest_stream = cache.read_artifact(&digest);
        deps.send_read_request_to_manifest_reader(manifest_stream, digest);
    }

    /// Called when a client finishes an upload of an artifact. We must notify the cache, which
    /// will attempt to incorporate the artifact. Then, depending on whether the cache is
    /// successful or not, we need to advance all affected jobs.
    pub fn receive_artifact_transferred(
        &mut self,
        cid: ClientId,
        digest: Sha256Digest,
        location: ArtifactUploadLocation,
    ) {
        let file = (location == ArtifactUploadLocation::TcpUpload)
            .then(|| self.tcp_upload_landing_pad.remove(&digest))
            .flatten();

        // We have to be careful with the jobs we get back from the cache. Currently, the cache
        // doesn't provide us a way to remove jobs that are waiting on cache reads. We can remove
        // a whole client, but not an individual job. This means that the jobs we get back from the
        // cache may no longer exist, as we may have encountered a failure looking reading another
        // artifact from cache or enumerating a manifest.
        match self.cache.got_artifact(&digest, file) {
            Err((err, jobs)) => {
                let client = self.clients.get_mut(&cid).unwrap();
                self.deps.send_general_error_to_client(
                    &mut client.sender,
                    format!("error incorporating artifact {digest} into cache: {err}"),
                );
                let jobs = jobs.into_iter().filter(|jid| {
                    // It's possible that the job failed for some other reason while we were
                    // waiting on the cache. Until we update the cache's API to allow us to cancel
                    // individual jobs, we have to deal with this.
                    if let Some(job) = self.try_pop_job(*jid) {
                        self.drop_job(*jid, job);
                        true
                    } else {
                        false
                    }
                });
                if let Some(jobs) = NonEmpty::collect(jobs) {
                    self.deps
                        .send_jobs_failed_to_scheduler(jobs, err.to_string());
                }
            }
            Ok(jobs) => {
                let ready = jobs.into_iter().filter(|jid| {
                    let client = self.clients.get_mut(&jid.cid).unwrap();
                    // It's possible that the job failed for some other reason while we were
                    // waiting on the cache. Until we update the cache's API to allow us to cancel
                    // individual jobs, we have to deal with this.
                    let Some(job) = client.jobs.get_mut(&jid.cjid) else {
                        return false;
                    };
                    let (digest_clone, is_manifest) =
                        job.artifacts_being_acquired.remove_entry(&digest).unwrap();
                    job.artifacts_acquired.insert(digest_clone).assert_is_true();
                    Self::potentially_start_reading_manifest_for_job(
                        &mut self.cache,
                        &mut client.sender,
                        &mut self.deps,
                        &digest,
                        is_manifest.map(|()| &mut self.manifest_reads),
                        *jid,
                        job,
                    );
                    job.have_all_artifacts()
                });
                if let Some(ready) = NonEmpty::collect(ready) {
                    self.deps.send_jobs_ready_to_scheduler(ready);
                }
            }
        }
    }

    /// Called when the manifest reader finds another entry in the manifest. We distribute the
    /// entry to all of the currently waiting jobs, and also save it in a set for any jobs added in
    /// the future.
    pub fn receive_manifest_entry(
        &mut self,
        manifest_digest: Sha256Digest,
        entry_digest: Sha256Digest,
    ) {
        let entry = self
            .manifest_reads
            .entries
            .get_mut(&manifest_digest)
            .unwrap();
        if entry.entries.insert(entry_digest.clone()) {
            for jid in &entry.jobs {
                let client = self.clients.get_mut(&jid.cid).unwrap();
                let job = client.jobs.get_mut(&jid.cjid).unwrap();
                Self::start_acquiring_artifact_for_job(
                    &mut self.cache,
                    &mut client.sender,
                    &mut self.deps,
                    entry_digest.clone(),
                    IsManifest::NotManifest,
                    *jid,
                    job,
                );
            }
        }
    }

    /// Called when the manifest reader has finished reading the whole manifest, or has encountered
    /// an error. We need to advance all jobs associated with the manifest.
    pub fn receive_finished_reading_manifest(
        &mut self,
        digest: Sha256Digest,
        result: anyhow::Result<()>,
    ) {
        assert!(self.manifest_reads.in_progress > 0);
        let ManifestReadEntry { entries, jobs } =
            self.manifest_reads.entries.remove(&digest).unwrap();
        self.manifest_reads.in_progress -= 1;
        match result {
            Err(err) => {
                for jid in &jobs {
                    let mut job = self.try_pop_job(*jid).unwrap();
                    job.manifests_being_read.remove(&digest).assert_is_true();
                    self.drop_job(*jid, job);
                }
                if let Some(jobs) = NonEmpty::collect(jobs) {
                    self.deps
                        .send_jobs_failed_to_scheduler(jobs, err.to_string());
                }
            }
            Ok(()) => {
                let ready = jobs.into_iter().filter(|jid| {
                    let client = self.clients.get_mut(&jid.cid).unwrap();
                    let job = client.jobs.get_mut(&jid.cjid).unwrap();
                    job.manifests_being_read.remove(&digest).assert_is_true();
                    job.have_all_artifacts()
                });
                if let Some(ready) = NonEmpty::collect(ready) {
                    self.deps.send_jobs_ready_to_scheduler(ready);
                }
                self.insert_manifest_into_cache(digest, entries);
            }
        }
        if let Some(digest) = self.manifest_reads.waiting.pop_front() {
            Self::start_manifest_reader(
                &mut self.cache,
                &mut self.deps,
                digest,
                &mut self.manifest_reads,
            );
        }
    }

    /// Called by the scheduler when a job has completed. The [`ArtifactGatherer`] releases all
    /// cache references for the job, and removes the job from its state.
    pub fn job_completed(&mut self, jid: JobId) {
        let job = self.try_pop_job(jid).unwrap();
        assert!(job.artifacts_being_acquired.is_empty());
        assert!(job.manifests_being_read.is_empty());
        self.drop_job(jid, job);
    }

    /// Called when the worker wants to read an artifact directly from the broker. We just call the
    /// cache and for the information back to the artifact fetcher.
    pub fn receive_get_artifact_for_worker(
        &mut self,
        digest: Sha256Digest,
        mut sender: DepsT::WorkerArtifactFetcherSender,
    ) {
        self.deps.send_response_to_worker_artifact_fetcher(
            &mut sender,
            self.cache.get_artifact_for_worker(&digest),
        );
    }

    /// Called when the worker is done reading an artifact directly from the broker. We just
    /// forward the call to the cache.
    pub fn receive_decrement_refcount_from_worker(&mut self, digest: Sha256Digest) {
        self.cache.decrement_refcount(&digest);
    }

    /// Return the number of jobs waiting on at least one artifact or reading at least one
    /// manifest, for a given client.
    pub fn get_waiting_for_artifacts_count(&self, cid: ClientId) -> u64 {
        self.clients
            .get(&cid)
            .unwrap()
            .jobs
            .values()
            .filter(|job| !job.have_all_artifacts())
            .count() as u64
    }

    /// Called when the clien artifact fetcher is done uploading an artifact.
    pub fn receive_got_artifact(&mut self, digest: Sha256Digest, file: CacheT::TempFile) {
        self.tcp_upload_landing_pad.insert(digest, file);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use anyhow::{anyhow, Error, Result};
    use std::{
        cell::RefCell,
        ops::{Deref, DerefMut},
        rc::Rc,
    };
    use ArtifactType::*;

    #[derive(Default)]
    struct Mock {
        // Deps.
        send_message_to_manifest_reader: HashSet<(i32, Sha256Digest)>,
        send_message_to_worker_artifact_fetcher: HashSet<(i32, Option<(PathBuf, u64)>)>,
        send_transfer_artifact_to_client: Vec<(ClientId, Sha256Digest)>,
        send_general_error_to_client: Vec<(ClientId, String)>,
        send_jobs_ready_to_scheduler: Vec<HashSet<JobId>>,
        send_jobs_failed_to_scheduler: Vec<(HashSet<JobId>, String)>,
        client_sender_dropped: HashSet<ClientId>,
        // Cache.
        get_artifact: HashMap<(JobId, Sha256Digest), GetArtifact>,
        got_artifact:
            HashMap<(Sha256Digest, Option<String>), Result<Vec<JobId>, (Error, Vec<JobId>)>>,
        decrement_refcount: Vec<Sha256Digest>,
        client_disconnected: HashSet<ClientId>,
        read_artifact: HashMap<Sha256Digest, i32>,
    }

    impl Mock {
        fn assert_is_empty(&self) {
            assert!(
                self.send_message_to_manifest_reader.is_empty(),
                "unused mock entries for Deps::send_message_to_manifest_reader: {:?}",
                self.send_message_to_manifest_reader,
            );
            assert!(
                self.send_message_to_worker_artifact_fetcher.is_empty(),
                "unused mock entries for Deps::send_message_to_worker_artifact_fetcher: {:?}",
                self.send_message_to_worker_artifact_fetcher,
            );
            assert!(
                self.send_transfer_artifact_to_client.is_empty(),
                "unused mock entries for Deps::send_transfer_artifact_to_client: {:?}",
                self.send_transfer_artifact_to_client,
            );
            assert!(
                self.send_general_error_to_client.is_empty(),
                "unused mock entries for Deps::send_general_error_to_client: {:?}",
                self.send_general_error_to_client,
            );
            assert!(
                self.send_jobs_ready_to_scheduler.is_empty(),
                "unused mock entries for Deps::send_jobs_ready_to_scheduler: {:?}",
                self.send_jobs_ready_to_scheduler,
            );
            assert!(
                self.send_jobs_failed_to_scheduler.is_empty(),
                "unused mock entries for Deps::send_job_failure_to_scheduler: {:?}",
                self.send_jobs_failed_to_scheduler,
            );
            assert!(
                self.client_sender_dropped.is_empty(),
                "unused mock entries for Deps::ClientSender::drop: {:?}",
                self.client_sender_dropped,
            );
            assert!(
                self.get_artifact.is_empty(),
                "unused mock entries for Cache::get_artifact: {:?}",
                self.get_artifact,
            );
            assert!(
                self.got_artifact.is_empty(),
                "unused mock entries for Cache::got_artifact: {:?}",
                self.got_artifact,
            );
            assert!(
                self.decrement_refcount.is_empty(),
                "unused mock entries for Cache::decrement_refcount: {:?}",
                self.decrement_refcount,
            );
            assert!(
                self.client_disconnected.is_empty(),
                "unused mock entries for Cache::client_disconnected: {:?}",
                self.client_disconnected,
            );
            assert!(
                self.read_artifact.is_empty(),
                "unused mock entries for Cache::read_artifact: {:?}",
                self.read_artifact,
            );
        }
    }

    struct TestClientSender {
        cid: ClientId,
        mock: Rc<RefCell<Mock>>,
    }

    impl Drop for TestClientSender {
        fn drop(&mut self) {
            assert!(
                self.mock
                    .borrow_mut()
                    .client_sender_dropped
                    .remove(&self.cid),
                "unexpected drop of client sender: {}",
                self.cid,
            );
        }
    }

    impl Deps for Rc<RefCell<Mock>> {
        type ArtifactStream = i32;
        type WorkerArtifactFetcherSender = i32;
        type ClientSender = TestClientSender;

        fn send_read_request_to_manifest_reader(
            &mut self,
            manifest_stream: Self::ArtifactStream,
            manifest_digest: Sha256Digest,
        ) {
            assert!(
                self.borrow_mut()
                    .send_message_to_manifest_reader
                    .remove(&(manifest_stream, manifest_digest.clone())),
                "sending unexpected message to manifest reader: {manifest_stream} {manifest_digest}"
            );
        }

        fn send_response_to_worker_artifact_fetcher(
            &mut self,
            sender: &mut Self::WorkerArtifactFetcherSender,
            message: Option<(PathBuf, u64)>,
        ) {
            self.borrow_mut()
                .send_message_to_worker_artifact_fetcher
                .remove(&(*sender, message))
                .assert_is_true();
        }

        fn send_transfer_artifact_to_client(
            &mut self,
            sender: &mut Self::ClientSender,
            digest: Sha256Digest,
        ) {
            let vec = &mut self.borrow_mut().send_transfer_artifact_to_client;
            let index = vec
                .iter()
                .position(|e| e.0 == sender.cid && e.1 == digest)
                .expect(&format!(
                    "sending unexpected transfer_artifact to client {cid}: {digest}",
                    cid = sender.cid,
                ));
            vec.remove(index);
        }

        fn send_general_error_to_client(&mut self, sender: &mut Self::ClientSender, error: String) {
            let vec = &mut self.borrow_mut().send_general_error_to_client;
            let index = vec
                .iter()
                .position(|e| e.0 == sender.cid && e.1 == error)
                .expect(&format!(
                    "sending unexpected general_error to client {cid}: {error}",
                    cid = sender.cid,
                ));
            let _ = vec.remove(index);
        }

        fn send_jobs_ready_to_scheduler(&mut self, jobs: NonEmpty<JobId>) {
            let jobs = HashSet::from_iter(jobs);
            let vec = &mut self.borrow_mut().send_jobs_ready_to_scheduler;
            let index = vec.iter().position(|e| e == &jobs).expect(&format!(
                "sending unexpected jobs_ready to scheduler: {jobs:?}"
            ));
            let _ = vec.remove(index);
        }

        fn send_jobs_failed_to_scheduler(&mut self, jobs: NonEmpty<JobId>, err: String) {
            let jobs = HashSet::from_iter(jobs);
            let vec = &mut self.borrow_mut().send_jobs_failed_to_scheduler;
            let index = vec
                .iter()
                .position(|e| e.0 == jobs && e.1 == err)
                .expect(&format!(
                    "sending unexpected jobs_failed to scheduler: {jobs:?} {err}"
                ));
            let _ = vec.remove(index);
        }
    }

    impl SchedulerCache for Rc<RefCell<Mock>> {
        type TempFile = String;
        type ArtifactStream = i32;

        fn get_artifact(&mut self, jid: JobId, digest: Sha256Digest) -> GetArtifact {
            self.borrow_mut()
                .get_artifact
                .remove(&(jid, digest))
                .expect(&format!(
                    "sending unexpected get_artifact to cache for {jid}"
                ))
        }

        fn got_artifact(
            &mut self,
            digest: &Sha256Digest,
            file: Option<Self::TempFile>,
        ) -> Result<Vec<JobId>, (Error, Vec<JobId>)> {
            self.borrow_mut()
                .got_artifact
                .remove(&(digest.clone(), file))
                .expect(&format!(
                    "sending unexpected got_artifact to cache: {digest}"
                ))
        }

        fn decrement_refcount(&mut self, digest: &Sha256Digest) {
            let vec = &mut self.borrow_mut().decrement_refcount;
            let index = vec.iter().position(|e| e == digest).expect(&format!(
                "sending unexpected decrement_refcount to cache: {digest}"
            ));
            vec.remove(index);
        }

        fn client_disconnected(&mut self, cid: ClientId) {
            self.borrow_mut()
                .client_disconnected
                .remove(&cid)
                .assert_is_true()
        }

        fn get_artifact_for_worker(&mut self, _digest: &Sha256Digest) -> Option<(PathBuf, u64)> {
            todo!();
        }

        fn read_artifact(&mut self, digest: &Sha256Digest) -> Self::ArtifactStream {
            self.borrow_mut().read_artifact.remove(digest).unwrap()
        }
    }

    struct Fixture {
        mock: Rc<RefCell<Mock>>,
        sut: ArtifactGatherer<Rc<RefCell<Mock>>, Rc<RefCell<Mock>>>,
        connected_clients: HashSet<ClientId>,
    }

    impl Drop for Fixture {
        fn drop(&mut self) {
            for cid in &self.connected_clients {
                self.mock
                    .borrow_mut()
                    .client_sender_dropped
                    .insert(*cid)
                    .assert_is_true();
            }
        }
    }

    impl Fixture {
        fn new() -> Self {
            Self::with_max_simultaneous_manifest_reads(100)
        }

        fn with_max_simultaneous_manifest_reads(max_simultaneous_manifest_reads: usize) -> Self {
            let mock = Rc::new(RefCell::new(Default::default()));
            let sut = ArtifactGatherer::new(
                mock.clone(),
                mock.clone(),
                1_000_000,
                max_simultaneous_manifest_reads.try_into().unwrap(),
            );
            Self {
                mock,
                sut,
                connected_clients: Default::default(),
            }
        }

        fn with_max_cache_size(max_cache_size: usize) -> Self {
            let mock = Rc::new(RefCell::new(Default::default()));
            let sut = ArtifactGatherer::new(
                mock.clone(),
                mock.clone(),
                max_cache_size,
                100.try_into().unwrap(),
            );
            Self {
                mock,
                sut,
                connected_clients: Default::default(),
            }
        }

        fn with_client(mut self, cid: impl Into<ClientId>) -> Self {
            let cid = cid.into();
            self.client_connected(cid);
            self.connected_clients.insert(cid).assert_is_true();
            self
        }

        fn expect(&mut self) -> Expect {
            Expect { fixture: self }
        }

        fn client_connected(&mut self, cid: impl Into<ClientId>) {
            let cid = cid.into();
            let sender = TestClientSender {
                cid,
                mock: self.mock.clone(),
            };
            self.sut.client_connected(cid, sender);
        }

        #[track_caller]
        fn start_job<LayersT, DigestT>(
            &mut self,
            jid: impl Into<JobId>,
            layers: LayersT,
            expected: StartJob,
        ) where
            LayersT: IntoIterator<Item = (DigestT, ArtifactType)>,
            Sha256Digest: From<DigestT>,
        {
            let actual = self.sut.start_job(
                jid.into(),
                NonEmpty::collect(
                    layers
                        .into_iter()
                        .map(|(digest, type_)| (digest.into(), type_)),
                )
                .unwrap(),
            );
            assert_eq!(actual, expected);
        }

        fn artifact_transferred(
            &mut self,
            cid: impl Into<ClientId>,
            digest: impl Into<Sha256Digest>,
            location: impl Into<ArtifactUploadLocation>,
        ) {
            self.sut
                .receive_artifact_transferred(cid.into(), digest.into(), location.into());
        }

        fn client_disconnected(&mut self, cid: impl Into<ClientId>) {
            self.sut.client_disconnected(cid.into());
        }

        fn receive_artifact_transferred(
            &mut self,
            cid: impl Into<ClientId>,
            digest: impl Into<Sha256Digest>,
            location: impl Into<ArtifactUploadLocation>,
        ) {
            self.sut
                .receive_artifact_transferred(cid.into(), digest.into(), location.into());
        }

        fn receive_manifest_entry(
            &mut self,
            manifest_digest: impl Into<Sha256Digest>,
            entry_digest: impl Into<Sha256Digest>,
        ) {
            self.sut
                .receive_manifest_entry(manifest_digest.into(), entry_digest.into());
        }

        fn receive_finished_reading_manifest(
            &mut self,
            digest: impl Into<Sha256Digest>,
            result: Result<()>,
        ) {
            self.sut
                .receive_finished_reading_manifest(digest.into(), result);
        }

        fn job_completed(&mut self, jid: impl Into<JobId>) {
            self.sut.job_completed(jid.into());
        }
    }

    struct Expect<'a> {
        fixture: &'a mut Fixture,
    }

    impl<'a> Drop for Expect<'a> {
        fn drop(&mut self) {
            self.fixture.mock.borrow().assert_is_empty();
        }
    }

    impl<'a> Expect<'a> {
        fn when(self) -> When<'a> {
            When { expect: self }
        }

        fn send_message_to_manifest_reader(
            self,
            digest: impl Into<Sha256Digest>,
            manifest_stream: i32,
        ) -> Self {
            self.fixture
                .mock
                .borrow_mut()
                .send_message_to_manifest_reader
                .insert((manifest_stream, digest.into()))
                .assert_is_true();
            self
        }

        fn send_transfer_artifact_to_client(
            self,
            cid: impl Into<ClientId>,
            digest: impl Into<Sha256Digest>,
        ) -> Self {
            self.fixture
                .mock
                .borrow_mut()
                .send_transfer_artifact_to_client
                .push((cid.into(), digest.into()));
            self
        }

        fn send_general_error_to_client(
            self,
            cid: impl Into<ClientId>,
            error: impl Into<String>,
        ) -> Self {
            self.fixture
                .mock
                .borrow_mut()
                .send_general_error_to_client
                .push((cid.into(), error.into()));
            self
        }

        fn send_jobs_ready_to_scheduler(
            self,
            jobs: impl IntoIterator<Item = impl Into<JobId>>,
        ) -> Self {
            self.fixture
                .mock
                .borrow_mut()
                .send_jobs_ready_to_scheduler
                .push(jobs.into_iter().map(Into::into).collect());
            self
        }

        fn send_jobs_failed_to_scheduler(
            self,
            jobs: impl IntoIterator<Item = impl Into<JobId>>,
            err: impl Into<String>,
        ) -> Self {
            self.fixture
                .mock
                .borrow_mut()
                .send_jobs_failed_to_scheduler
                .push((jobs.into_iter().map(Into::into).collect(), err.into()));
            self
        }

        fn get_artifact(
            self,
            jid: impl Into<JobId>,
            digest: impl Into<Sha256Digest>,
            result: GetArtifact,
        ) -> Self {
            self.fixture
                .mock
                .borrow_mut()
                .get_artifact
                .insert((jid.into(), digest.into()), result)
                .assert_is_none();
            self
        }

        fn got_artifact_success(
            self,
            digest: impl Into<Sha256Digest>,
            file: Option<&str>,
            jobs: impl IntoIterator<Item = impl Into<JobId>>,
        ) -> Self {
            self.fixture
                .mock
                .borrow_mut()
                .got_artifact
                .insert(
                    (digest.into(), file.map(Into::into)),
                    Ok(jobs.into_iter().map(Into::into).collect()),
                )
                .assert_is_none();
            self
        }

        fn got_artifact_failure(
            self,
            digest: impl Into<Sha256Digest>,
            file: Option<&str>,
            error: impl Into<String>,
            jobs: impl IntoIterator<Item = impl Into<JobId>>,
        ) -> Self {
            self.fixture
                .mock
                .borrow_mut()
                .got_artifact
                .insert(
                    (digest.into(), file.map(Into::into)),
                    Err((
                        anyhow!(error.into()),
                        jobs.into_iter().map(Into::into).collect(),
                    )),
                )
                .assert_is_none();
            self
        }

        fn decrement_refcount(self, digest: impl Into<Sha256Digest>) -> Self {
            self.fixture
                .mock
                .borrow_mut()
                .decrement_refcount
                .push(digest.into());
            self
        }

        fn client_disconnected(self, cid: impl Into<ClientId>) -> Self {
            self.fixture
                .mock
                .borrow_mut()
                .client_disconnected
                .insert(cid.into())
                .assert_is_true();
            self
        }

        fn client_sender_dropped(self, cid: impl Into<ClientId>) -> Self {
            self.fixture
                .mock
                .borrow_mut()
                .client_sender_dropped
                .insert(cid.into())
                .assert_is_true();
            self
        }

        fn read_artifact(self, digest: impl Into<Sha256Digest>, result: i32) -> Self {
            self.fixture
                .mock
                .borrow_mut()
                .read_artifact
                .insert(digest.into(), result)
                .assert_is_none();
            self
        }
    }

    struct When<'a> {
        expect: Expect<'a>,
    }

    impl<'a> Deref for When<'a> {
        type Target = Fixture;

        fn deref(&self) -> &Self::Target {
            &self.expect.fixture
        }
    }

    impl<'a> DerefMut for When<'a> {
        fn deref_mut(&mut self) -> &mut Self::Target {
            &mut self.expect.fixture
        }
    }

    #[test]
    #[should_panic]
    fn duplicate_client_connected_panics() {
        let mut fixture = Fixture::new().with_client(1);
        fixture
            .expect()
            .client_sender_dropped(1)
            .when()
            .client_connected(1);
    }

    #[test]
    #[should_panic]
    fn unknown_client_disconnect_panics() {
        let mut fixture = Fixture::new().with_client(1);
        fixture.client_disconnected(2);
    }

    #[test]
    fn client_disconnect_no_jobs() {
        let mut fixture = Fixture::new().with_client(1);
        fixture
            .expect()
            .client_disconnected(1)
            .client_sender_dropped(1)
            .when()
            .client_disconnected(1);
    }

    #[test]
    fn client_disconnect_jobs_with_some_artifacts() {
        let mut fixture = Fixture::new().with_client(2);
        fixture.client_connected(1);
        fixture
            .expect()
            .get_artifact((1, 1), 1, GetArtifact::Success)
            .get_artifact((1, 1), 2, GetArtifact::Wait)
            .get_artifact((1, 1), 3, GetArtifact::Get)
            .get_artifact((1, 1), 4, GetArtifact::Success)
            .read_artifact(4, 44)
            .send_message_to_manifest_reader(4, 44)
            .send_transfer_artifact_to_client(1, 3)
            .when()
            .start_job(
                (1, 1),
                [(1, Tar), (2, Tar), (3, Tar), (4, Manifest), (1, Tar)],
                StartJob::NotReady,
            );
        fixture
            .expect()
            .get_artifact((1, 2), 1, GetArtifact::Success)
            .when()
            .start_job((1, 2), [(1, Tar)], StartJob::Ready);
        fixture
            .expect()
            .client_disconnected(1)
            .client_sender_dropped(1)
            .decrement_refcount(1)
            .decrement_refcount(1)
            .decrement_refcount(4)
            .when()
            .client_disconnected(1);
    }

    #[test]
    #[should_panic]
    fn start_job_for_unknown_client_panics() {
        let mut fixture = Fixture::new().with_client(2);
        fixture.start_job((1, 1), [(1, Tar)], StartJob::Ready);
    }

    #[test]
    #[should_panic(expected = "job entry already exists for 1.2")]
    fn start_job_with_duplicate_jid_panics() {
        let mut fixture = Fixture::new().with_client(1);
        fixture
            .expect()
            .get_artifact((1, 2), 1, GetArtifact::Success)
            .when()
            .start_job((1, 2), [(1, Tar)], StartJob::Ready);
        fixture.start_job((1, 2), [(1, Tar)], StartJob::Ready);
    }

    #[test]
    fn start_job_get_tar_artifact_success() {
        let mut fixture = Fixture::new().with_client(1);
        fixture
            .expect()
            .get_artifact((1, 2), 3, GetArtifact::Success)
            .when()
            .start_job((1, 2), [(3, Tar)], StartJob::Ready);
    }

    #[test]
    fn start_job_duplicate_artifacts_get_tar_artifact_success() {
        let mut fixture = Fixture::new().with_client(1);
        fixture
            .expect()
            .get_artifact((1, 2), 3, GetArtifact::Success)
            .when()
            .start_job((1, 2), [(3, Tar), (3, Tar)], StartJob::Ready);
    }

    #[test]
    fn start_job_get_tar_artifact_get() {
        let mut fixture = Fixture::new().with_client(1);
        fixture
            .expect()
            .get_artifact((1, 2), 3, GetArtifact::Get)
            .send_transfer_artifact_to_client(1, 3)
            .when()
            .start_job((1, 2), [(3, Tar)], StartJob::NotReady);
    }

    #[test]
    fn start_job_duplicate_artifacts_get_tar_artifact_get() {
        let mut fixture = Fixture::new().with_client(1);
        fixture
            .expect()
            .get_artifact((1, 2), 3, GetArtifact::Get)
            .send_transfer_artifact_to_client(1, 3)
            .when()
            .start_job((1, 2), [(3, Tar), (3, Tar)], StartJob::NotReady);
    }

    #[test]
    fn start_job_get_tar_artifact_wait() {
        let mut fixture = Fixture::new().with_client(1);
        fixture
            .expect()
            .get_artifact((1, 2), 3, GetArtifact::Wait)
            .when()
            .start_job((1, 2), [(3, Tar)], StartJob::NotReady);
    }

    #[test]
    fn start_job_duplicate_artifacts_get_tar_artifact_wait() {
        let mut fixture = Fixture::new().with_client(1);
        fixture
            .expect()
            .get_artifact((1, 2), 3, GetArtifact::Wait)
            .when()
            .start_job((1, 2), [(3, Tar), (3, Tar)], StartJob::NotReady);
    }

    #[test]
    fn start_job_get_manifest_artifact_success() {
        let mut fixture = Fixture::new().with_client(1);
        fixture
            .expect()
            .get_artifact((1, 2), 3, GetArtifact::Success)
            .read_artifact(3, 33)
            .send_message_to_manifest_reader(3, 33)
            .when()
            .start_job((1, 2), [(3, Manifest)], StartJob::NotReady);
    }

    #[test]
    fn start_job_duplicate_artifacts_get_manifest_artifact_success() {
        let mut fixture = Fixture::new().with_client(1);
        fixture
            .expect()
            .get_artifact((1, 2), 3, GetArtifact::Success)
            .read_artifact(3, 33)
            .send_message_to_manifest_reader(3, 33)
            .when()
            .start_job((1, 2), [(3, Manifest), (3, Manifest)], StartJob::NotReady);
    }

    #[test]
    fn start_job_get_manifest_artifact_get() {
        let mut fixture = Fixture::new().with_client(1);
        fixture
            .expect()
            .get_artifact((1, 2), 3, GetArtifact::Get)
            .send_transfer_artifact_to_client(1, 3)
            .when()
            .start_job((1, 2), [(3, Manifest)], StartJob::NotReady);
    }

    #[test]
    fn start_job_duplicate_artifacts_get_manifest_artifact_get() {
        let mut fixture = Fixture::new().with_client(1);
        fixture
            .expect()
            .get_artifact((1, 2), 3, GetArtifact::Get)
            .send_transfer_artifact_to_client(1, 3)
            .when()
            .start_job((1, 2), [(3, Manifest), (3, Manifest)], StartJob::NotReady);
    }

    #[test]
    fn start_job_get_manifest_artifact_wait() {
        let mut fixture = Fixture::new().with_client(1);
        fixture
            .expect()
            .get_artifact((1, 2), 3, GetArtifact::Wait)
            .when()
            .start_job((1, 2), [(3, Manifest)], StartJob::NotReady);
    }

    #[test]
    fn start_job_duplicate_artifacts_get_manifest_artifact_wait() {
        let mut fixture = Fixture::new().with_client(1);
        fixture
            .expect()
            .get_artifact((1, 2), 3, GetArtifact::Wait)
            .when()
            .start_job((1, 2), [(3, Manifest), (3, Manifest)], StartJob::NotReady);
    }

    #[test]
    fn receive_artifact_transferred_success_not_tcp_no_jobs() {
        let mut fixture = Fixture::new().with_client(1);
        fixture
            .expect()
            .got_artifact_success(2, None, [] as [JobId; 0])
            .when()
            .receive_artifact_transferred(1, 2, ArtifactUploadLocation::Remote);
    }

    #[test]
    fn receive_artifact_transferred_success_not_tcp_some_jobs_only_some_are_ready() {
        let mut fixture = Fixture::new().with_client(1);
        fixture
            .expect()
            .get_artifact((1, 2), 5, GetArtifact::Get)
            .send_transfer_artifact_to_client(1, 5)
            .when()
            .start_job((1, 2), [(5, Tar)], StartJob::NotReady);
        fixture
            .expect()
            .get_artifact((1, 3), 5, GetArtifact::Wait)
            .get_artifact((1, 3), 6, GetArtifact::Get)
            .get_artifact((1, 3), 7, GetArtifact::Get)
            .send_transfer_artifact_to_client(1, 6)
            .send_transfer_artifact_to_client(1, 7)
            .when()
            .start_job((1, 3), [(5, Tar), (6, Tar), (7, Tar)], StartJob::NotReady);
        fixture
            .expect()
            .got_artifact_success(5, None, [(1, 2), (1, 3)])
            .send_jobs_ready_to_scheduler([(1, 2)])
            .when()
            .receive_artifact_transferred(1, 5, ArtifactUploadLocation::Remote);
        fixture
            .expect()
            .got_artifact_success(6, None, [(1, 3)])
            .when()
            .receive_artifact_transferred(1, 6, ArtifactUploadLocation::Remote);
        fixture
            .expect()
            .got_artifact_success(7, None, [(1, 3)])
            .send_jobs_ready_to_scheduler([(1, 3)])
            .when()
            .receive_artifact_transferred(1, 7, ArtifactUploadLocation::Remote);
    }

    #[test]
    fn receive_artifact_transferred_failure_not_tcp_no_jobs() {
        let mut fixture = Fixture::new().with_client(1);
        fixture
            .expect()
            .got_artifact_failure(2, None, "error", [] as [JobId; 0])
            .send_general_error_to_client(
                1,
                format!(
                    "error incorporating artifact {} into cache: error",
                    Sha256Digest::from(2)
                ),
            )
            .when()
            .receive_artifact_transferred(1, 2, ArtifactUploadLocation::Remote);
    }

    #[test]
    fn receive_artifact_transferred_failure_not_tcp_only_fails_jobs_for_all_clients() {
        // The idea here is that if the cache fails has a failure doing something internally, it
        // should fail all the jobs: it's not a client-specific thing.
        let mut fixture = Fixture::new().with_client(1).with_client(2);
        fixture
            .expect()
            .get_artifact((1, 2), 5, GetArtifact::Get)
            .send_transfer_artifact_to_client(1, 5)
            .when()
            .start_job((1, 2), [(5, Tar)], StartJob::NotReady);
        fixture
            .expect()
            .get_artifact((1, 3), 5, GetArtifact::Wait)
            .get_artifact((1, 3), 6, GetArtifact::Get)
            .get_artifact((1, 3), 7, GetArtifact::Get)
            .send_transfer_artifact_to_client(1, 6)
            .send_transfer_artifact_to_client(1, 7)
            .when()
            .start_job((1, 3), [(5, Tar), (6, Tar), (7, Tar)], StartJob::NotReady);
        fixture
            .expect()
            .get_artifact((2, 2), 5, GetArtifact::Get)
            .send_transfer_artifact_to_client(2, 5)
            .when()
            .start_job((2, 2), [(5, Tar)], StartJob::NotReady);
        fixture
            .expect()
            .got_artifact_failure(5, None, "error", [(1, 2), (1, 3), (2, 2)])
            .send_general_error_to_client(
                1,
                format!(
                    "error incorporating artifact {} into cache: error",
                    Sha256Digest::from(5)
                ),
            )
            .send_jobs_failed_to_scheduler([(1, 2), (1, 3), (2, 2)], "error")
            .when()
            .receive_artifact_transferred(1, 5, ArtifactUploadLocation::Remote);
    }

    #[test]
    fn manifest_read_for_job_entry_from_disconnected_client() {
        let mut fixture = Fixture::new().with_client(1);
        fixture
            .expect()
            .get_artifact((1, 2), 3, GetArtifact::Success)
            .read_artifact(3, 33)
            .send_message_to_manifest_reader(3, 33)
            .when()
            .start_job((1, 2), [(3, Manifest)], StartJob::NotReady);
        fixture
            .expect()
            .client_disconnected(1)
            .client_sender_dropped(1)
            .decrement_refcount(3)
            .when()
            .client_disconnected(1);
        fixture.receive_manifest_entry(3, 4);
    }

    #[test]
    fn manifest_read_for_job_complete_from_disconnected_client() {
        let mut fixture = Fixture::new().with_client(1);
        fixture
            .expect()
            .get_artifact((1, 2), 3, GetArtifact::Success)
            .read_artifact(3, 33)
            .send_message_to_manifest_reader(3, 33)
            .when()
            .start_job((1, 2), [(3, Manifest)], StartJob::NotReady);
        fixture
            .expect()
            .client_disconnected(1)
            .client_sender_dropped(1)
            .decrement_refcount(3)
            .when()
            .client_disconnected(1);
        fixture.receive_finished_reading_manifest(3, Ok(()));
    }

    #[test]
    fn manifest_read_for_job_entry_various_cache_states() {
        let mut fixture = Fixture::new().with_client(1);
        fixture
            .expect()
            .get_artifact((1, 2), 3, GetArtifact::Success)
            .read_artifact(3, 33)
            .send_message_to_manifest_reader(3, 33)
            .when()
            .start_job((1, 2), [(3, Manifest)], StartJob::NotReady);
        fixture
            .expect()
            .get_artifact((1, 2), 4, GetArtifact::Success)
            .when()
            .receive_manifest_entry(3, 4);
        fixture.receive_manifest_entry(3, 4);
        fixture
            .expect()
            .get_artifact((1, 2), 5, GetArtifact::Wait)
            .when()
            .receive_manifest_entry(3, 5);
        fixture.receive_manifest_entry(3, 5);
        fixture
            .expect()
            .send_transfer_artifact_to_client(1, 6)
            .get_artifact((1, 2), 6, GetArtifact::Get)
            .when()
            .receive_manifest_entry(3, 6);
        fixture.receive_manifest_entry(3, 6);
        fixture.receive_finished_reading_manifest(3, Ok(()));
        fixture
            .expect()
            .got_artifact_success(6, None, [(1, 2)])
            .when()
            .artifact_transferred(1, 6, ArtifactUploadLocation::Remote);
        fixture
            .expect()
            .got_artifact_success(5, None, [(1, 2)])
            .send_jobs_ready_to_scheduler([(1, 2)])
            .when()
            .artifact_transferred(1, 5, ArtifactUploadLocation::Remote);
    }

    #[test]
    fn artifact_tranferred_ok_for_multiple_jobs() {
        let mut fixture = Fixture::new().with_client(1).with_client(2);
        fixture
            .expect()
            .get_artifact((1, 2), 3, GetArtifact::Get)
            .send_transfer_artifact_to_client(1, 3)
            .when()
            .start_job((1, 2), [(3, Tar)], StartJob::NotReady);
        fixture
            .expect()
            .get_artifact((2, 2), 3, GetArtifact::Wait)
            .when()
            .start_job((2, 2), [(3, Tar)], StartJob::NotReady);
        fixture
            .expect()
            .got_artifact_success(3, None, [(1, 2), (2, 2)])
            .send_jobs_ready_to_scheduler([(1, 2), (2, 2)])
            .when()
            .artifact_transferred(1, 3, ArtifactUploadLocation::Remote);
    }

    #[test]
    fn artifact_tranferred_ok_kicks_off_manifest_read() {
        let mut fixture = Fixture::new().with_client(1);
        fixture
            .expect()
            .get_artifact((1, 2), 3, GetArtifact::Get)
            .send_transfer_artifact_to_client(1, 3)
            .when()
            .start_job((1, 2), [(3, Manifest)], StartJob::NotReady);
        fixture
            .expect()
            .got_artifact_success(3, None, [(1, 2)])
            .read_artifact(3, 33)
            .send_message_to_manifest_reader(3, 33)
            .when()
            .artifact_transferred(1, 3, ArtifactUploadLocation::Remote);
    }

    #[test]
    fn manifest_read_for_job_complete_tracks_count_for_job() {
        let mut fixture = Fixture::new().with_client(1);
        fixture
            .expect()
            .get_artifact((1, 2), 3, GetArtifact::Success)
            .get_artifact((1, 2), 4, GetArtifact::Success)
            .read_artifact(3, 33)
            .send_message_to_manifest_reader(3, 33)
            .read_artifact(4, 44)
            .send_message_to_manifest_reader(4, 44)
            .when()
            .start_job((1, 2), [(3, Manifest), (4, Manifest)], StartJob::NotReady);
        fixture.receive_finished_reading_manifest(3, Ok(()));
        fixture
            .expect()
            .send_jobs_ready_to_scheduler([(1, 2)])
            .when()
            .receive_finished_reading_manifest(4, Ok(()));
    }

    #[test]
    fn complete_job_one_artifact() {
        let mut fixture = Fixture::new().with_client(1);
        fixture
            .expect()
            .get_artifact((1, 2), 3, GetArtifact::Success)
            .when()
            .start_job((1, 2), [(3, Tar)], StartJob::Ready);
        fixture
            .expect()
            .decrement_refcount(3)
            .when()
            .job_completed((1, 2));
    }

    #[test]
    fn complete_job_two_artifacts() {
        let mut fixture = Fixture::new().with_client(1);
        fixture
            .expect()
            .get_artifact((1, 2), 3, GetArtifact::Success)
            .get_artifact((1, 2), 4, GetArtifact::Success)
            .when()
            .start_job((1, 2), [(3, Tar), (4, Tar)], StartJob::Ready);
        fixture
            .expect()
            .decrement_refcount(3)
            .decrement_refcount(4)
            .when()
            .job_completed((1, 2));
    }

    #[test]
    fn complete_job_duplicate_artifacts() {
        let mut fixture = Fixture::new().with_client(1);
        fixture
            .expect()
            .get_artifact((1, 2), 3, GetArtifact::Success)
            .when()
            .start_job((1, 2), [(3, Tar), (3, Tar)], StartJob::Ready);
        fixture
            .expect()
            .decrement_refcount(3)
            .when()
            .job_completed((1, 2));
    }

    #[test]
    fn reading_multiple_manifests_simultaneously_and_successfully_with_all_in_cache() {
        let mut fixture = Fixture::new().with_client(1).with_client(2);
        fixture
            .expect()
            .get_artifact((1, 2), 3, GetArtifact::Success)
            .read_artifact(3, 103)
            .send_message_to_manifest_reader(3, 103)
            .when()
            .start_job((1, 2), [(3, Manifest), (3, Manifest)], StartJob::NotReady);

        // Other manifest should act independently.
        fixture
            .expect()
            .get_artifact((2, 2), 4, GetArtifact::Success)
            .read_artifact(4, 104)
            .send_message_to_manifest_reader(4, 104)
            .when()
            .start_job((2, 2), [(4, Manifest), (4, Manifest)], StartJob::NotReady);

        fixture
            .expect()
            .get_artifact((1, 2), 30, GetArtifact::Success)
            .when()
            .receive_manifest_entry(3, 30);

        fixture
            .expect()
            .get_artifact((2, 1), 3, GetArtifact::Success)
            .get_artifact((2, 1), 30, GetArtifact::Success)
            .when()
            .start_job((2, 1), [(3, Manifest), (3, Manifest)], StartJob::NotReady);

        fixture
            .expect()
            .get_artifact((1, 2), 31, GetArtifact::Success)
            .get_artifact((2, 1), 31, GetArtifact::Success)
            .when()
            .receive_manifest_entry(3, 31);

        // When we finish reading, we should continue with both jobs.
        fixture
            .expect()
            .send_jobs_ready_to_scheduler([(1, 2), (2, 1)])
            .when()
            .receive_finished_reading_manifest(3, Ok(()));
    }

    #[test]
    fn simultaneous_manifest_reads_limited() {
        let mut fixture = Fixture::with_max_simultaneous_manifest_reads(1).with_client(1);

        fixture
            .expect()
            .get_artifact((1, 1), 3, GetArtifact::Success)
            .get_artifact((1, 1), 4, GetArtifact::Success)
            .read_artifact(3, 103)
            .send_message_to_manifest_reader(3, 103)
            .when()
            .start_job((1, 1), [(3, Manifest), (4, Manifest)], StartJob::NotReady);
        fixture
            .expect()
            .get_artifact((1, 2), 4, GetArtifact::Success)
            .get_artifact((1, 2), 5, GetArtifact::Success)
            .when()
            .start_job((1, 2), [(4, Manifest), (5, Manifest)], StartJob::NotReady);
        fixture
            .expect()
            .get_artifact((1, 1), 30, GetArtifact::Success)
            .when()
            .receive_manifest_entry(3, 30);
        fixture
            .expect()
            .read_artifact(4, 104)
            .send_message_to_manifest_reader(4, 104)
            .when()
            .receive_finished_reading_manifest(3, Ok(()));
        fixture
            .expect()
            .get_artifact((1, 1), 40, GetArtifact::Success)
            .get_artifact((1, 2), 40, GetArtifact::Success)
            .when()
            .receive_manifest_entry(4, 40);
        fixture
            .expect()
            .read_artifact(5, 105)
            .send_message_to_manifest_reader(5, 105)
            .send_jobs_ready_to_scheduler([(1, 1)])
            .when()
            .receive_finished_reading_manifest(4, Ok(()));
        fixture
            .expect()
            .get_artifact((1, 2), 50, GetArtifact::Success)
            .when()
            .receive_manifest_entry(5, 50);
        fixture
            .expect()
            .send_jobs_ready_to_scheduler([(1, 2)])
            .when()
            .receive_finished_reading_manifest(5, Ok(()));
    }

    #[test]
    fn manifest_reads_are_cached() {
        let mut fixture = Fixture::new().with_client(1);

        fixture
            .expect()
            .get_artifact((1, 1), 3, GetArtifact::Success)
            .get_artifact((1, 1), 4, GetArtifact::Success)
            .read_artifact(3, 103)
            .send_message_to_manifest_reader(3, 103)
            .read_artifact(4, 104)
            .send_message_to_manifest_reader(4, 104)
            .when()
            .start_job((1, 1), [(3, Manifest), (4, Manifest)], StartJob::NotReady);
        fixture
            .expect()
            .get_artifact((1, 2), 4, GetArtifact::Success)
            .get_artifact((1, 2), 5, GetArtifact::Success)
            .read_artifact(5, 105)
            .send_message_to_manifest_reader(5, 105)
            .when()
            .start_job((1, 2), [(4, Manifest), (5, Manifest)], StartJob::NotReady);

        fixture
            .expect()
            .get_artifact((1, 1), 30, GetArtifact::Success)
            .when()
            .receive_manifest_entry(3, 30);
        fixture.receive_finished_reading_manifest(3, Ok(()));

        fixture
            .expect()
            .get_artifact((1, 1), 40, GetArtifact::Success)
            .get_artifact((1, 2), 40, GetArtifact::Success)
            .when()
            .receive_manifest_entry(4, 40);
        fixture
            .expect()
            .get_artifact((1, 1), 41, GetArtifact::Success)
            .get_artifact((1, 2), 41, GetArtifact::Success)
            .when()
            .receive_manifest_entry(4, 41);
        fixture
            .expect()
            .send_jobs_ready_to_scheduler([(1, 1)])
            .when()
            .receive_finished_reading_manifest(4, Ok(()));

        fixture
            .expect()
            .get_artifact((1, 2), 50, GetArtifact::Success)
            .when()
            .receive_manifest_entry(5, 50);
        fixture
            .expect()
            .send_jobs_ready_to_scheduler([(1, 2)])
            .when()
            .receive_finished_reading_manifest(5, Ok(()));

        fixture
            .expect()
            .get_artifact((1, 3), 3, GetArtifact::Success)
            .get_artifact((1, 3), 4, GetArtifact::Success)
            .get_artifact((1, 3), 5, GetArtifact::Success)
            .get_artifact((1, 3), 30, GetArtifact::Success)
            .get_artifact((1, 3), 40, GetArtifact::Success)
            .get_artifact((1, 3), 41, GetArtifact::Success)
            .get_artifact((1, 3), 50, GetArtifact::Success)
            .when()
            .start_job(
                (1, 3),
                [(3, Manifest), (4, Manifest), (5, Manifest)],
                StartJob::Ready,
            );
    }

    #[test]
    fn manifest_reads_max_cache_size_0() {
        let mut fixture = Fixture::with_max_cache_size(0).with_client(1);

        fixture
            .expect()
            .get_artifact((1, 1), 3, GetArtifact::Success)
            .read_artifact(3, 103)
            .send_message_to_manifest_reader(3, 103)
            .when()
            .start_job((1, 1), [(3, Manifest)], StartJob::NotReady);
        fixture
            .expect()
            .get_artifact((1, 1), 30, GetArtifact::Success)
            .when()
            .receive_manifest_entry(3, 30);
        fixture
            .expect()
            .get_artifact((1, 1), 31, GetArtifact::Success)
            .when()
            .receive_manifest_entry(3, 31);
        fixture
            .expect()
            .send_jobs_ready_to_scheduler([(1, 1)])
            .when()
            .receive_finished_reading_manifest(3, Ok(()));

        fixture
            .expect()
            .get_artifact((1, 2), 3, GetArtifact::Success)
            .read_artifact(3, 103)
            .send_message_to_manifest_reader(3, 103)
            .when()
            .start_job((1, 2), [(3, Manifest)], StartJob::NotReady);
    }

    #[test]
    fn manifest_reads_cache_lru() {
        let mut fixture = Fixture::with_max_cache_size(336).with_client(1);

        fixture
            .expect()
            .get_artifact((1, 1), 3, GetArtifact::Success)
            .read_artifact(3, 103)
            .send_message_to_manifest_reader(3, 103)
            .when()
            .start_job((1, 1), [(3, Manifest)], StartJob::NotReady);
        fixture
            .expect()
            .get_artifact((1, 1), 30, GetArtifact::Success)
            .when()
            .receive_manifest_entry(3, 30);
        fixture
            .expect()
            .get_artifact((1, 1), 31, GetArtifact::Success)
            .when()
            .receive_manifest_entry(3, 31);
        fixture
            .expect()
            .send_jobs_ready_to_scheduler([(1, 1)])
            .when()
            .receive_finished_reading_manifest(3, Ok(()));

        assert_eq!(fixture.sut.manifest_reads.cache_size, 168);

        fixture
            .expect()
            .get_artifact((1, 2), 4, GetArtifact::Success)
            .read_artifact(4, 104)
            .send_message_to_manifest_reader(4, 104)
            .when()
            .start_job((1, 2), [(4, Manifest)], StartJob::NotReady);
        fixture
            .expect()
            .get_artifact((1, 2), 40, GetArtifact::Success)
            .when()
            .receive_manifest_entry(4, 40);
        fixture
            .expect()
            .get_artifact((1, 2), 41, GetArtifact::Success)
            .when()
            .receive_manifest_entry(4, 41);
        fixture
            .expect()
            .send_jobs_ready_to_scheduler([(1, 2)])
            .when()
            .receive_finished_reading_manifest(4, Ok(()));

        assert_eq!(fixture.sut.manifest_reads.cache_size, 336);

        fixture
            .expect()
            .get_artifact((1, 3), 3, GetArtifact::Success)
            .get_artifact((1, 3), 30, GetArtifact::Success)
            .get_artifact((1, 3), 31, GetArtifact::Success)
            .when()
            .start_job((1, 3), [(3, Manifest)], StartJob::Ready);

        fixture
            .expect()
            .get_artifact((1, 4), 5, GetArtifact::Success)
            .read_artifact(5, 105)
            .send_message_to_manifest_reader(5, 105)
            .when()
            .start_job((1, 4), [(5, Manifest)], StartJob::NotReady);
        fixture
            .expect()
            .get_artifact((1, 4), 50, GetArtifact::Success)
            .when()
            .receive_manifest_entry(5, 50);
        fixture
            .expect()
            .get_artifact((1, 4), 51, GetArtifact::Success)
            .when()
            .receive_manifest_entry(5, 51);
        fixture
            .expect()
            .send_jobs_ready_to_scheduler([(1, 4)])
            .when()
            .receive_finished_reading_manifest(5, Ok(()));

        assert_eq!(fixture.sut.manifest_reads.cache_size, 336);

        fixture
            .expect()
            .get_artifact((1, 5), 3, GetArtifact::Success)
            .get_artifact((1, 5), 30, GetArtifact::Success)
            .get_artifact((1, 5), 31, GetArtifact::Success)
            .when()
            .start_job((1, 5), [(3, Manifest)], StartJob::Ready);

        fixture
            .expect()
            .get_artifact((1, 6), 5, GetArtifact::Success)
            .get_artifact((1, 6), 50, GetArtifact::Success)
            .get_artifact((1, 6), 51, GetArtifact::Success)
            .when()
            .start_job((1, 6), [(5, Manifest)], StartJob::Ready);

        fixture
            .expect()
            .get_artifact((1, 7), 4, GetArtifact::Success)
            .read_artifact(4, 104)
            .send_message_to_manifest_reader(4, 104)
            .when()
            .start_job((1, 7), [(4, Manifest)], StartJob::NotReady);
    }
}
