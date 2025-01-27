use crate::{cache::SchedulerCache, scheduler_task::ManifestReadRequest};
use maelstrom_base::{
    proto::ArtifactUploadLocation, ArtifactType, ClientId, ClientJobId, JobId, NonEmpty,
    Sha256Digest,
};
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
    type WorkerArtifactFetcherSender;
    type ClientSender;
    fn send_message_to_manifest_reader(
        &mut self,
        request: ManifestReadRequest<Self::ArtifactStream>,
    );
    fn send_message_to_worker_artifact_fetcher(
        &mut self,
        sender: &mut Self::WorkerArtifactFetcherSender,
        message: Option<(PathBuf, u64)>,
    );
    fn send_transfer_artifact_to_client(
        &mut self,
        sender: &mut Self::ClientSender,
        digest: Sha256Digest,
    );
    fn send_artifact_transferred_response_to_client(
        &mut self,
        sender: &mut Self::ClientSender,
        digest: Sha256Digest,
        result: Result<(), String>,
    );
    fn send_job_ready_to_scheduler(&mut self, jid: JobId);
    fn send_job_failure_to_scheduler(&mut self, jid: JobId, err: String);
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
    NotReady,
    Ready,
}

pub struct ArtifactGatherer<CacheT: SchedulerCache, DepsT: Deps> {
    cache: CacheT,
    deps: DepsT,
    clients: HashMap<ClientId, Client<DepsT>>,
    tcp_upload_landing_pad: HashMap<Sha256Digest, CacheT::TempFile>,
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
            tcp_upload_landing_pad: Default::default(),
        }
    }

    fn receive_client_connected(&mut self, cid: ClientId, sender: DepsT::ClientSender) {
        self.clients
            .insert(cid, Client::new(sender))
            .assert_is_none();
    }

    fn receive_client_disconnected(&mut self, cid: ClientId) {
        self.cache.client_disconnected(cid);

        let client = self.clients.remove(&cid).unwrap();
        for job in client.jobs.into_values() {
            for artifact in job.acquired_artifacts {
                self.cache.decrement_refcount(&artifact);
            }
        }
    }

    fn start_artifact_acquisition_for_job(
        cache: &mut CacheT,
        client_sender: &mut DepsT::ClientSender,
        deps: &mut DepsT,
        digest: &Sha256Digest,
        is_manifest: IsManifest,
        jid: JobId,
        job: &mut Job,
    ) {
        if job.acquired_artifacts.contains(digest) || job.missing_artifacts.contains_key(digest) {
            return;
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
            }
            GetArtifact::Wait => {
                job.missing_artifacts
                    .insert(digest.clone(), is_manifest)
                    .assert_is_none();
            }
            GetArtifact::Get => {
                job.missing_artifacts
                    .insert(digest.clone(), is_manifest)
                    .assert_is_none();
                deps.send_transfer_artifact_to_client(client_sender, digest.clone());
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

        for (digest, type_) in layers {
            let is_manifest = IsManifest::from(type_ == ArtifactType::Manifest);
            Self::start_artifact_acquisition_for_job(
                &mut self.cache,
                &mut client.sender,
                &mut self.deps,
                &digest,
                is_manifest,
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

    pub fn receive_artifact_transferred(
        &mut self,
        cid: ClientId,
        digest: Sha256Digest,
        location: ArtifactUploadLocation,
    ) {
        let file = (location == ArtifactUploadLocation::TcpUpload)
            .then(|| self.tcp_upload_landing_pad.remove(&digest))
            .flatten();

        let client = self.clients.get_mut(&cid).unwrap();
        match self.cache.got_artifact(&digest, file) {
            Err(err) => {
                self.deps.send_artifact_transferred_response_to_client(
                    &mut client.sender,
                    digest,
                    Err(err.to_string()),
                );
                Default::default()
            }
            Ok(jids) => {
                self.deps.send_artifact_transferred_response_to_client(
                    &mut client.sender,
                    digest.clone(),
                    Ok(()),
                );
                for jid in jids {
                    let client = self.clients.get_mut(&jid.cid).unwrap();
                    let job = client.jobs.get_mut(&jid.cjid).unwrap();
                    let is_manifest = job.missing_artifacts.remove(&digest).unwrap();
                    Self::complete_artifact_acquisition_for_job(
                        &mut self.cache,
                        &mut self.deps,
                        &digest,
                        is_manifest,
                        jid,
                        job,
                    );
                    if job.have_all_artifacts() {
                        self.deps.send_job_ready_to_scheduler(jid);
                    }
                }
            }
        }
    }

    pub fn receive_manifest_entry(&mut self, digest: Sha256Digest, jid: JobId) {
        let Some(client) = self.clients.get_mut(&jid.cid) else {
            // This indicates that the client isn't around anymore. Just ignore this message. When
            // the client disconnected, we canceled all of the outstanding requests. Ideally, we
            // would have a way to cancel the outstanding manifest read, but we don't currently
            // have that.
            return;
        };
        let job = client.jobs.get_mut(&jid.cjid).unwrap();
        Self::start_artifact_acquisition_for_job(
            &mut self.cache,
            &mut client.sender,
            &mut self.deps,
            &digest,
            IsManifest::NotManifest,
            jid,
            job,
        );
    }

    pub fn receive_finished_reading_manifest(
        &mut self,
        digest: Sha256Digest,
        jid: JobId,
        result: anyhow::Result<()>,
    ) {
        match result {
            Ok(()) => {}
            Err(err) => {
                self.deps
                    .send_job_failure_to_scheduler(jid, err.to_string());
                return;
            }
        }

        let Some(client) = self.clients.get_mut(&jid.cid) else {
            // This indicates that the client isn't around anymore. Just ignore this message. When
            // the client disconnected, we canceled all of the outstanding requests.
            return;
        };
        let job = client.jobs.get_mut(&jid.cjid).unwrap();
        job.manifests_being_read.remove(&digest).assert_is_true();

        if job.have_all_artifacts() {
            self.deps.send_job_ready_to_scheduler(jid);
        }
    }

    fn complete_job(&mut self, jid: JobId) {
        let client = self.clients.get_mut(&jid.cid).unwrap();
        let job = client.jobs.remove(&jid.cjid).unwrap();
        for artifact in job.acquired_artifacts {
            self.cache.decrement_refcount(&artifact);
        }
    }

    pub fn receive_get_artifact_for_worker(
        &mut self,
        digest: Sha256Digest,
        mut sender: DepsT::WorkerArtifactFetcherSender,
    ) {
        self.deps.send_message_to_worker_artifact_fetcher(
            &mut sender,
            self.cache.get_artifact_for_worker(&digest),
        );
    }

    pub fn receive_decrement_refcount_from_worker(&mut self, digest: Sha256Digest) {
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

    pub fn receive_got_artifact(&mut self, digest: Sha256Digest, file: CacheT::TempFile) {
        self.tcp_upload_landing_pad.insert(digest, file);
    }
}

impl<CacheT, DepsT> super::scheduler::ArtifactGatherer for ArtifactGatherer<CacheT, DepsT>
where
    CacheT: SchedulerCache,
    DepsT: Deps<ArtifactStream = CacheT::ArtifactStream>,
{
    type ClientSender = DepsT::ClientSender;

    fn receive_client_connected(&mut self, cid: ClientId, sender: Self::ClientSender) {
        self.receive_client_connected(cid, sender)
    }

    fn receive_client_disconnected(&mut self, cid: ClientId) {
        self.receive_client_disconnected(cid)
    }

    fn start_job(
        &mut self,
        jid: JobId,
        layers: NonEmpty<(Sha256Digest, ArtifactType)>,
    ) -> StartJob {
        self.start_job(jid, layers)
    }

    fn receive_job_completed(&mut self, jid: JobId) {
        self.complete_job(jid)
    }

    fn get_waiting_for_artifacts_count(&self, cid: ClientId) -> u64 {
        self.get_waiting_for_artifacts_count(cid)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use anyhow::Result;
    use std::{
        cell::RefCell,
        ops::{Deref, DerefMut},
        rc::Rc,
    };
    use ArtifactType::*;

    #[derive(Default)]
    struct Mock {
        // Deps.
        send_message_to_manifest_reader: HashSet<ManifestReadRequest<i32>>,
        send_message_to_worker_artifact_fetcher: HashSet<(i32, Option<(PathBuf, u64)>)>,
        send_transfer_artifact_to_client: Vec<(ClientId, Sha256Digest)>,
        send_artifact_transferred_response_to_client:
            Vec<(ClientId, Sha256Digest, Result<(), String>)>,
        send_job_ready_to_scheduler: HashSet<JobId>,
        client_sender_dropped: HashSet<ClientId>,
        // Cache.
        get_artifact: HashMap<(JobId, Sha256Digest), GetArtifact>,
        got_artifact: HashMap<(Sha256Digest, Option<String>), Result<Vec<JobId>>>,
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
                self.send_artifact_transferred_response_to_client.is_empty(),
                "unused mock entries for Deps::send_artifact_transferred_response_to_client: {:?}",
                self.send_artifact_transferred_response_to_client,
            );
            assert!(
                self.send_job_ready_to_scheduler.is_empty(),
                "unused mock entries for Deps::send_job_ready_to_scheduler: {:?}",
                self.send_job_ready_to_scheduler,
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

        fn send_message_to_manifest_reader(
            &mut self,
            request: ManifestReadRequest<Self::ArtifactStream>,
        ) {
            assert!(
                self.borrow_mut()
                    .send_message_to_manifest_reader
                    .remove(&request),
                "sending unexpected message to manifest reader: {request:#?}"
            );
        }

        fn send_message_to_worker_artifact_fetcher(
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

        fn send_artifact_transferred_response_to_client(
            &mut self,
            sender: &mut Self::ClientSender,
            digest: Sha256Digest,
            result: Result<(), String>,
        ) {
            let vec = &mut self
                .borrow_mut()
                .send_artifact_transferred_response_to_client;
            let index = vec
                .iter()
                .position(|e| e.0 == sender.cid && e.1 == digest && e.2 == result)
                .expect(&format!(
                    "sending unexpected artifact_transferred_response to client {cid}: {digest}",
                    cid = sender.cid,
                ));
            let _ = vec.remove(index);
        }

        fn send_job_ready_to_scheduler(&mut self, jid: JobId) {
            self.borrow_mut()
                .send_job_ready_to_scheduler
                .remove(&jid)
                .assert_is_true();
        }

        fn send_job_failure_to_scheduler(&mut self, jid: JobId, err: String) {
            todo!("{jid} {err}");
        }
    }

    impl SchedulerCache for Rc<RefCell<Mock>> {
        type TempFile = String;
        type ArtifactStream = i32;

        fn get_artifact(&mut self, jid: JobId, digest: Sha256Digest) -> GetArtifact {
            self.borrow_mut()
                .get_artifact
                .remove(&(jid, digest))
                .unwrap()
        }

        fn got_artifact(
            &mut self,
            digest: &Sha256Digest,
            file: Option<Self::TempFile>,
        ) -> Result<Vec<JobId>> {
            self.borrow_mut()
                .got_artifact
                .remove(&(digest.clone(), file))
                .unwrap()
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

    impl Default for Fixture {
        fn default() -> Self {
            let mock = Rc::new(RefCell::new(Default::default()));
            let sut = ArtifactGatherer::new(mock.clone(), mock.clone());
            Self {
                mock,
                sut,
                connected_clients: Default::default(),
            }
        }
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
        fn with_clients(cids: impl IntoIterator<Item = impl Into<ClientId>>) -> Self {
            let mut fixture = Self::default();
            fixture.connected_clients = cids.into_iter().map(Into::into).collect();
            for cid in fixture.connected_clients.clone() {
                fixture.client_connected(cid);
            }
            fixture
        }

        fn with_client(cid: impl Into<ClientId>) -> Self {
            Self::with_clients([cid])
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
            self.sut.receive_client_connected(cid, sender);
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
            self.sut.receive_client_disconnected(cid.into());
        }

        fn receive_manifest_entry(
            &mut self,
            digest: impl Into<Sha256Digest>,
            jid: impl Into<JobId>,
        ) {
            self.sut.receive_manifest_entry(digest.into(), jid.into());
        }

        fn receive_finished_reading_manifest(
            &mut self,
            digest: impl Into<Sha256Digest>,
            jid: impl Into<JobId>,
            result: Result<()>,
        ) {
            self.sut
                .receive_finished_reading_manifest(digest.into(), jid.into(), result);
        }

        fn complete_job(&mut self, jid: impl Into<JobId>) {
            self.sut.complete_job(jid.into());
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
            jid: impl Into<JobId>,
            digest: impl Into<Sha256Digest>,
            manifest_stream: i32,
        ) -> Self {
            self.fixture
                .mock
                .borrow_mut()
                .send_message_to_manifest_reader
                .insert(ManifestReadRequest {
                    jid: jid.into(),
                    digest: digest.into(),
                    manifest_stream,
                })
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

        fn send_artifact_transferred_response_to_client(
            self,
            cid: impl Into<ClientId>,
            digest: impl Into<Sha256Digest>,
            result: Result<(), String>,
        ) -> Self {
            self.fixture
                .mock
                .borrow_mut()
                .send_artifact_transferred_response_to_client
                .push((cid.into(), digest.into(), result));
            self
        }

        fn send_job_ready_to_scheduler(self, jid: impl Into<JobId>) -> Self {
            self.fixture
                .mock
                .borrow_mut()
                .send_job_ready_to_scheduler
                .insert(jid.into())
                .assert_is_true();
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

        fn got_artifact<DigestT, JobIdIterT, JobIdT>(
            self,
            digest: DigestT,
            file: Option<&str>,
            result: Result<JobIdIterT>,
        ) -> Self
        where
            Sha256Digest: From<DigestT>,
            JobId: From<JobIdT>,
            JobIdIterT: IntoIterator<Item = JobIdT>,
        {
            self.fixture
                .mock
                .borrow_mut()
                .got_artifact
                .insert(
                    (digest.into(), file.map(Into::into)),
                    result.map(|iter| iter.into_iter().map(Into::into).collect()),
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
    fn start_job_get_tar_artifact_success() {
        let mut fixture = Fixture::with_client(1);
        fixture
            .expect()
            .get_artifact((1, 2), 3, GetArtifact::Success)
            .when()
            .start_job((1, 2), [(3, Tar)], StartJob::Ready);
    }

    #[test]
    fn start_job_duplicate_artifacts_get_tar_artifact_success() {
        let mut fixture = Fixture::with_client(1);
        fixture
            .expect()
            .get_artifact((1, 2), 3, GetArtifact::Success)
            .when()
            .start_job((1, 2), [(3, Tar), (3, Tar)], StartJob::Ready);
    }

    #[test]
    fn start_job_get_tar_artifact_get() {
        let mut fixture = Fixture::with_client(1);
        fixture
            .expect()
            .get_artifact((1, 2), 3, GetArtifact::Get)
            .send_transfer_artifact_to_client(1, 3)
            .when()
            .start_job((1, 2), [(3, Tar)], StartJob::NotReady);
        fixture
            .expect()
            .got_artifact(3, None, Ok([(1, 2)]))
            .send_artifact_transferred_response_to_client(1, 3, Ok(()))
            .send_job_ready_to_scheduler((1, 2))
            .when()
            .artifact_transferred(1, 3, ArtifactUploadLocation::Remote);
    }

    #[test]
    fn start_job_duplicate_artifacts_get_tar_artifact_get() {
        let mut fixture = Fixture::with_client(1);
        fixture
            .expect()
            .get_artifact((1, 2), 3, GetArtifact::Get)
            .send_transfer_artifact_to_client(1, 3)
            .when()
            .start_job((1, 2), [(3, Tar), (3, Tar)], StartJob::NotReady);
        fixture
            .expect()
            .got_artifact(3, None, Ok([(1, 2)]))
            .send_artifact_transferred_response_to_client(1, 3, Ok(()))
            .send_job_ready_to_scheduler((1, 2))
            .when()
            .artifact_transferred(1, 3, ArtifactUploadLocation::Remote);
    }

    #[test]
    fn start_job_get_tar_artifact_wait() {
        let mut fixture = Fixture::with_client(1);
        fixture
            .expect()
            .get_artifact((1, 2), 3, GetArtifact::Wait)
            .when()
            .start_job((1, 2), [(3, Tar)], StartJob::NotReady);
    }

    #[test]
    fn start_job_duplicate_artifacts_get_tar_artifact_wait() {
        let mut fixture = Fixture::with_client(1);
        fixture
            .expect()
            .get_artifact((1, 2), 3, GetArtifact::Wait)
            .when()
            .start_job((1, 2), [(3, Tar), (3, Tar)], StartJob::NotReady);
    }

    #[test]
    fn start_job_get_manifest_artifact_success() {
        let mut fixture = Fixture::with_client(1);
        fixture
            .expect()
            .get_artifact((1, 2), 3, GetArtifact::Success)
            .read_artifact(3, 33)
            .send_message_to_manifest_reader((1, 2), 3, 33)
            .when()
            .start_job((1, 2), [(3, Manifest)], StartJob::NotReady);
    }

    #[test]
    fn start_job_duplicate_artifacts_get_manifest_artifact_success() {
        let mut fixture = Fixture::with_client(1);
        fixture
            .expect()
            .get_artifact((1, 2), 3, GetArtifact::Success)
            .read_artifact(3, 33)
            .send_message_to_manifest_reader((1, 2), 3, 33)
            .when()
            .start_job((1, 2), [(3, Manifest), (3, Manifest)], StartJob::NotReady);
    }

    #[test]
    fn start_job_get_manifest_artifact_get() {
        let mut fixture = Fixture::with_client(1);
        fixture
            .expect()
            .get_artifact((1, 2), 3, GetArtifact::Get)
            .send_transfer_artifact_to_client(1, 3)
            .when()
            .start_job((1, 2), [(3, Manifest)], StartJob::NotReady);
    }

    #[test]
    fn start_job_duplicate_artifacts_get_manifest_artifact_get() {
        let mut fixture = Fixture::with_client(1);
        fixture
            .expect()
            .get_artifact((1, 2), 3, GetArtifact::Get)
            .send_transfer_artifact_to_client(1, 3)
            .when()
            .start_job((1, 2), [(3, Manifest), (3, Manifest)], StartJob::NotReady);
    }

    #[test]
    fn start_job_get_manifest_artifact_wait() {
        let mut fixture = Fixture::with_client(1);
        fixture
            .expect()
            .get_artifact((1, 2), 3, GetArtifact::Wait)
            .when()
            .start_job((1, 2), [(3, Manifest)], StartJob::NotReady);
    }

    #[test]
    fn start_job_duplicate_artifacts_get_manifest_artifact_wait() {
        let mut fixture = Fixture::with_client(1);
        fixture
            .expect()
            .get_artifact((1, 2), 3, GetArtifact::Wait)
            .when()
            .start_job((1, 2), [(3, Manifest), (3, Manifest)], StartJob::NotReady);
    }

    #[test]
    fn manifest_read_for_job_entry_from_disconnected_client() {
        let mut fixture = Fixture::with_client(1);
        fixture
            .expect()
            .get_artifact((1, 2), 3, GetArtifact::Success)
            .read_artifact(3, 33)
            .send_message_to_manifest_reader((1, 2), 3, 33)
            .when()
            .start_job((1, 2), [(3, Manifest)], StartJob::NotReady);
        fixture
            .expect()
            .client_disconnected(1)
            .client_sender_dropped(1)
            .decrement_refcount(3)
            .when()
            .client_disconnected(1);
        fixture.receive_manifest_entry(4, (1, 2));
    }

    #[test]
    fn manifest_read_for_job_complete_from_disconnected_client() {
        let mut fixture = Fixture::with_client(1);
        fixture
            .expect()
            .get_artifact((1, 2), 3, GetArtifact::Success)
            .read_artifact(3, 33)
            .send_message_to_manifest_reader((1, 2), 3, 33)
            .when()
            .start_job((1, 2), [(3, Manifest)], StartJob::NotReady);
        fixture
            .expect()
            .client_disconnected(1)
            .client_sender_dropped(1)
            .decrement_refcount(3)
            .when()
            .client_disconnected(1);
        fixture.receive_finished_reading_manifest(3, (1, 2), Ok(()));
    }

    #[test]
    fn manifest_read_for_job_entry_various_cache_states() {
        let mut fixture = Fixture::with_client(1);
        fixture
            .expect()
            .get_artifact((1, 2), 3, GetArtifact::Success)
            .read_artifact(3, 33)
            .send_message_to_manifest_reader((1, 2), 3, 33)
            .when()
            .start_job((1, 2), [(3, Manifest)], StartJob::NotReady);
        fixture
            .expect()
            .get_artifact((1, 2), 4, GetArtifact::Success)
            .when()
            .receive_manifest_entry(4, (1, 2));
        fixture.receive_manifest_entry(4, (1, 2));
        fixture
            .expect()
            .get_artifact((1, 2), 5, GetArtifact::Wait)
            .when()
            .receive_manifest_entry(5, (1, 2));
        fixture.receive_manifest_entry(5, (1, 2));
        fixture
            .expect()
            .get_artifact((1, 2), 6, GetArtifact::Get)
            .send_transfer_artifact_to_client(1, 6)
            .when()
            .receive_manifest_entry(6, (1, 2));
        fixture.receive_manifest_entry(6, (1, 2));
        fixture.receive_finished_reading_manifest(3, (1, 2), Ok(()));
        fixture
            .expect()
            .got_artifact(6, None, Ok([(1, 2)]))
            .send_artifact_transferred_response_to_client(1, 6, Ok(()))
            .when()
            .artifact_transferred(1, 6, ArtifactUploadLocation::Remote);
        fixture
            .expect()
            .got_artifact(5, None, Ok([(1, 2)]))
            .send_artifact_transferred_response_to_client(1, 5, Ok(()))
            .send_job_ready_to_scheduler((1, 2))
            .when()
            .artifact_transferred(1, 5, ArtifactUploadLocation::Remote);
    }

    #[test]
    fn artifact_tranferred_ok_for_multiple_jobs() {
        let mut fixture = Fixture::with_clients([1, 2]);
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
            .got_artifact(3, None, Ok([(1, 2), (2, 2)]))
            .send_artifact_transferred_response_to_client(1, 3, Ok(()))
            .send_job_ready_to_scheduler((1, 2))
            .send_job_ready_to_scheduler((2, 2))
            .when()
            .artifact_transferred(1, 3, ArtifactUploadLocation::Remote);
    }

    #[test]
    fn artifact_tranferred_ok_kicks_off_manifest_read() {
        let mut fixture = Fixture::with_client(1);
        fixture
            .expect()
            .get_artifact((1, 2), 3, GetArtifact::Get)
            .send_transfer_artifact_to_client(1, 3)
            .when()
            .start_job((1, 2), [(3, Manifest)], StartJob::NotReady);
        fixture
            .expect()
            .got_artifact(3, None, Ok([(1, 2)]))
            .send_artifact_transferred_response_to_client(1, 3, Ok(()))
            .read_artifact(3, 33)
            .send_message_to_manifest_reader((1, 2), 3, 33)
            .when()
            .artifact_transferred(1, 3, ArtifactUploadLocation::Remote);
    }

    #[test]
    fn manifest_read_for_job_complete_tracks_count_for_job() {
        let mut fixture = Fixture::with_client(1);
        fixture
            .expect()
            .get_artifact((1, 2), 3, GetArtifact::Success)
            .get_artifact((1, 2), 4, GetArtifact::Success)
            .read_artifact(3, 33)
            .send_message_to_manifest_reader((1, 2), 3, 33)
            .read_artifact(4, 44)
            .send_message_to_manifest_reader((1, 2), 4, 44)
            .when()
            .start_job((1, 2), [(3, Manifest), (4, Manifest)], StartJob::NotReady);
        fixture.receive_finished_reading_manifest(3, (1, 2), Ok(()));
        fixture
            .expect()
            .send_job_ready_to_scheduler((1, 2))
            .when()
            .receive_finished_reading_manifest(4, (1, 2), Ok(()));
    }

    #[test]
    fn complete_job_one_artifact() {
        let mut fixture = Fixture::with_client(1);
        fixture
            .expect()
            .get_artifact((1, 2), 3, GetArtifact::Success)
            .when()
            .start_job((1, 2), [(3, Tar)], StartJob::Ready);
        fixture
            .expect()
            .decrement_refcount(3)
            .when()
            .complete_job((1, 2));
    }

    #[test]
    fn complete_job_two_artifacts() {
        let mut fixture = Fixture::with_client(1);
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
            .complete_job((1, 2));
    }

    #[test]
    fn complete_job_duplicate_artifacts() {
        let mut fixture = Fixture::with_client(1);
        fixture
            .expect()
            .get_artifact((1, 2), 3, GetArtifact::Success)
            .when()
            .start_job((1, 2), [(3, Tar), (3, Tar)], StartJob::Ready);
        fixture
            .expect()
            .decrement_refcount(3)
            .when()
            .complete_job((1, 2));
    }

    #[test]
    fn client_disconnect_no_jobs() {
        let mut fixture = Fixture::with_client(1);
        fixture
            .expect()
            .client_disconnected(1)
            .client_sender_dropped(1)
            .when()
            .client_disconnected(1);
    }

    #[test]
    fn client_disconnect_jobs_with_some_artifacts() {
        let mut fixture = Fixture::default();
        fixture.client_connected(1);
        fixture
            .expect()
            .get_artifact((1, 1), 1, GetArtifact::Success)
            .get_artifact((1, 1), 2, GetArtifact::Wait)
            .get_artifact((1, 1), 3, GetArtifact::Get)
            .get_artifact((1, 1), 4, GetArtifact::Success)
            .read_artifact(4, 44)
            .send_message_to_manifest_reader((1, 1), 4, 44)
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
        fixture.client_connected(2);
        fixture
            .expect()
            .get_artifact((2, 2), 1, GetArtifact::Success)
            .when()
            .start_job((2, 2), [(1, Tar)], StartJob::Ready);
        fixture
            .expect()
            .client_disconnected(1)
            .client_sender_dropped(1)
            .decrement_refcount(1)
            .decrement_refcount(1)
            .decrement_refcount(4)
            .when()
            .client_disconnected(1);
        fixture
            .expect()
            .client_disconnected(2)
            .client_sender_dropped(2)
            .decrement_refcount(1)
            .when()
            .client_disconnected(2);
    }
}
