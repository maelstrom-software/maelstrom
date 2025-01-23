use crate::{cache::SchedulerCache, scheduler_task::ManifestReadRequest};
use anyhow::Result;
use maelstrom_base::{
    proto::BrokerToClient, ArtifactType, ClientId, ClientJobId, JobId, NonEmpty, Sha256Digest,
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
    fn send_message_to_client(&mut self, sender: &mut Self::ClientSender, message: BrokerToClient);
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

    fn client_connected(&mut self, cid: ClientId, sender: DepsT::ClientSender) {
        self.clients
            .insert(cid, Client::new(sender))
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
                deps.send_message_to_client(
                    client_sender,
                    BrokerToClient::TransferArtifact(digest.clone()),
                );
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

    fn manifest_read_for_job_entry(&mut self, digest: &Sha256Digest, jid: JobId) {
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
            digest,
            IsManifest::NotManifest,
            jid,
            job,
        );
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
}

impl<CacheT, DepsT> super::scheduler::ArtifactGatherer for ArtifactGatherer<CacheT, DepsT>
where
    CacheT: SchedulerCache,
    DepsT: Deps<ArtifactStream = CacheT::ArtifactStream>,
{
    type TempFile = CacheT::TempFile;
    type ClientSender = DepsT::ClientSender;

    fn client_connected(&mut self, cid: ClientId, sender: Self::ClientSender) {
        self.client_connected(cid, sender)
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

    fn manifest_read_for_job_entry(&mut self, digest: &Sha256Digest, jid: JobId) {
        self.manifest_read_for_job_entry(digest, jid);
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

    fn get_waiting_for_artifacts_count(&self, cid: ClientId) -> u64 {
        self.get_waiting_for_artifacts_count(cid)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use hashbag::HashBag;
    use maelstrom_base::digest;
    use std::{
        cell::{RefCell, RefMut},
        rc::Rc,
    };
    use ArtifactType::*;

    #[derive(Default)]
    struct TestStateInner {
        send_message_to_manifest_reader_returns: HashSet<ManifestReadRequest<i32>>,
        send_message_to_worker_artifact_fetcher_returns: HashSet<(i32, Option<(PathBuf, u64)>)>,
        send_message_to_client_returns: Vec<(ClientId, BrokerToClient)>,
        get_artifact_returns: HashMap<(JobId, Sha256Digest), GetArtifact>,
        got_artifact_returns: HashMap<(Sha256Digest, Option<String>), Result<Vec<JobId>>>,
        decrement_refcount_returns: HashBag<Sha256Digest>,
        client_disconnected_returns: HashSet<ClientId>,
        read_artifact_returns: HashMap<Sha256Digest, i32>,
    }

    impl Drop for TestStateInner {
        fn drop(&mut self) {
            assert!(
                self.send_message_to_manifest_reader_returns.is_empty(),
                "unused test fixture entries for Deps::send_message_to_manifest_reader: {:?}",
                self.send_message_to_manifest_reader_returns,
            );
            assert!(
                self.send_message_to_worker_artifact_fetcher_returns.is_empty(),
                "unused test fixture entries for Deps::send_message_to_worker_artifact_fetcher: {:?}",
                self.send_message_to_worker_artifact_fetcher_returns,
            );
            assert!(
                self.send_message_to_client_returns.is_empty(),
                "unused test fixture entries for Deps::send_message_to_client: {:?}",
                self.send_message_to_client_returns,
            );
            assert!(
                self.get_artifact_returns.is_empty(),
                "unused test fixture entries for Cache::get_artifact: {:?}",
                self.get_artifact_returns,
            );
            assert!(
                self.got_artifact_returns.is_empty(),
                "unused test fixture entries for Cache::got_artifact: {:?}",
                self.got_artifact_returns,
            );
            assert!(
                self.decrement_refcount_returns.is_empty(),
                "unused test fixture entries for Cache::decrement_refcount: {:?}",
                self.decrement_refcount_returns,
            );
            assert!(
                self.client_disconnected_returns.is_empty(),
                "unused test fixture entries for Cache::client_disconnected: {:?}",
                self.client_disconnected_returns,
            );
            assert!(
                self.read_artifact_returns.is_empty(),
                "unused test fixture entries for Cache::read_artifact: {:?}",
                self.read_artifact_returns,
            );
        }
    }

    struct Expect<'a> {
        test_state: RefMut<'a, TestState>,
    }

    impl<'a> Expect<'a> {
        fn send_message_to_manifest_reader(
            mut self,
            jid: impl Into<JobId>,
            digest: impl Into<Sha256Digest>,
            manifest_stream: i32,
        ) -> Self {
            self.test_state
                .inner
                .as_mut()
                .unwrap()
                .send_message_to_manifest_reader_returns
                .insert(ManifestReadRequest {
                    jid: jid.into(),
                    digest: digest.into(),
                    manifest_stream,
                })
                .assert_is_true();
            self
        }

        fn send_message_to_client(
            mut self,
            cid: impl Into<ClientId>,
            message: BrokerToClient,
        ) -> Self {
            self.test_state
                .inner
                .as_mut()
                .unwrap()
                .send_message_to_client_returns
                .push((cid.into(), message));
            self
        }

        fn get_artifact(
            mut self,
            jid: impl Into<JobId>,
            digest: impl Into<Sha256Digest>,
            result: GetArtifact,
        ) -> Self {
            self.test_state
                .inner
                .as_mut()
                .unwrap()
                .get_artifact_returns
                .insert((jid.into(), digest.into()), result)
                .assert_is_none();
            self
        }

        fn got_artifact<DigestT, TempFileT, JobIdIterT, JobIdT>(
            mut self,
            digest: DigestT,
            file: TempFileT,
            result: Result<JobIdIterT>,
        ) -> Self
        where
            Sha256Digest: From<DigestT>,
            String: From<TempFileT>,
            JobId: From<JobIdT>,
            JobIdIterT: IntoIterator<Item = JobIdT>,
        {
            self.test_state
                .inner
                .as_mut()
                .unwrap()
                .got_artifact_returns
                .insert(
                    (digest.into(), Some(file.into())),
                    result.map(|iter| iter.into_iter().map(Into::into).collect()),
                )
                .assert_is_none();
            self
        }

        fn decrement_refcount(mut self, digest: impl Into<Sha256Digest>) -> Self {
            self.test_state
                .inner
                .as_mut()
                .unwrap()
                .decrement_refcount_returns
                .insert(digest.into());
            self
        }

        fn client_disconnected(mut self, cid: impl Into<ClientId>) -> Self {
            self.test_state
                .inner
                .as_mut()
                .unwrap()
                .client_disconnected_returns
                .insert(cid.into())
                .assert_is_true();
            self
        }

        fn read_artifact(mut self, digest: impl Into<Sha256Digest>, result: i32) -> Self {
            self.test_state
                .inner
                .as_mut()
                .unwrap()
                .read_artifact_returns
                .insert(digest.into(), result)
                .assert_is_none();
            self
        }
    }

    #[derive(Default)]
    struct TestState {
        inner: Option<TestStateInner>,
    }

    impl Deps for Rc<RefCell<TestState>> {
        type ArtifactStream = i32;
        type WorkerArtifactFetcherSender = i32;
        type ClientSender = ClientId;

        fn send_message_to_manifest_reader(
            &mut self,
            request: ManifestReadRequest<Self::ArtifactStream>,
        ) {
            self.borrow_mut()
                .inner
                .as_mut()
                .unwrap()
                .send_message_to_manifest_reader_returns
                .remove(&request)
                .assert_is_true();
        }

        fn send_message_to_worker_artifact_fetcher(
            &mut self,
            sender: &mut Self::WorkerArtifactFetcherSender,
            message: Option<(PathBuf, u64)>,
        ) {
            self.borrow_mut()
                .inner
                .as_mut()
                .unwrap()
                .send_message_to_worker_artifact_fetcher_returns
                .remove(&(*sender, message))
                .assert_is_true();
        }

        fn send_message_to_client(
            &mut self,
            sender: &mut Self::ClientSender,
            message: BrokerToClient,
        ) {
            let mut borrow = self.borrow_mut();
            let send_message_to_client = &mut borrow
                .inner
                .as_mut()
                .unwrap()
                .send_message_to_client_returns;
            let index = send_message_to_client
                .iter()
                .position(|e| e.0 == *sender && e.1 == message)
                .expect(&format!(
                    "sending unexpected message to client {sender}: {message:#?}"
                ));
            send_message_to_client.remove(index);
        }
    }

    impl SchedulerCache for Rc<RefCell<TestState>> {
        type TempFile = String;
        type ArtifactStream = i32;

        fn get_artifact(&mut self, jid: JobId, digest: Sha256Digest) -> GetArtifact {
            self.borrow_mut()
                .inner
                .as_mut()
                .unwrap()
                .get_artifact_returns
                .remove(&(jid, digest))
                .unwrap()
        }

        fn got_artifact(
            &mut self,
            digest: &Sha256Digest,
            file: Option<Self::TempFile>,
        ) -> Result<Vec<JobId>> {
            self.borrow_mut()
                .inner
                .as_mut()
                .unwrap()
                .got_artifact_returns
                .remove(&(digest.clone(), file))
                .unwrap()
        }

        fn decrement_refcount(&mut self, digest: &Sha256Digest) {
            let refcount_before = self
                .borrow_mut()
                .inner
                .as_mut()
                .unwrap()
                .decrement_refcount_returns
                .remove(&digest);
            assert!(refcount_before > 0);
        }

        fn client_disconnected(&mut self, cid: ClientId) {
            self.borrow_mut()
                .inner
                .as_mut()
                .unwrap()
                .client_disconnected_returns
                .remove(&cid)
                .assert_is_true()
        }

        fn get_artifact_for_worker(&mut self, _digest: &Sha256Digest) -> Option<(PathBuf, u64)> {
            todo!();
        }

        fn read_artifact(&mut self, digest: &Sha256Digest) -> Self::ArtifactStream {
            self.borrow_mut()
                .inner
                .as_mut()
                .unwrap()
                .read_artifact_returns
                .remove(digest)
                .unwrap()
        }
    }

    struct Fixture {
        test_state: Rc<RefCell<TestState>>,
        sut: ArtifactGatherer<Rc<RefCell<TestState>>, Rc<RefCell<TestState>>>,
    }

    impl Fixture {
        fn new() -> Self {
            let test_state = Rc::new(RefCell::new(Default::default()));
            let sut = ArtifactGatherer::new(test_state.clone(), test_state.clone());
            Self { test_state, sut }
        }

        fn expect(&mut self) -> Expect {
            let mut test_state = self.test_state.borrow_mut();
            test_state.inner.replace(Default::default());
            Expect { test_state }
        }

        #[track_caller]
        fn client_connected(&mut self, cid: impl Into<ClientId>) {
            let cid = cid.into();
            self.sut.client_connected(cid, cid);
            self.test_state.take();
        }

        #[track_caller]
        fn client_disconnected(&mut self, cid: impl Into<ClientId>) {
            self.sut.client_disconnected(cid.into());
            self.test_state.take();
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
            self.test_state.take();
        }

        #[track_caller]
        fn complete_job(&mut self, jid: impl Into<JobId>) {
            self.sut.complete_job(jid.into());
            self.test_state.take();
        }

        #[track_caller]
        fn artifact_transferred_ok<JobIdT>(
            &mut self,
            digest: impl Into<Sha256Digest>,
            file: impl Into<String>,
            expected: impl IntoIterator<Item = JobIdT>,
        ) where
            JobIdT: Into<JobId>,
        {
            let actual = self
                .sut
                .artifact_transferred(&digest.into(), Some(file.into()))
                .unwrap();
            assert_eq!(actual, expected.into_iter().map(Into::into).collect());
            self.test_state.take();
        }

        fn manifest_read_for_job_entry(
            &mut self,
            digest: impl Into<Sha256Digest>,
            jid: impl Into<JobId>,
        ) {
            self.sut
                .manifest_read_for_job_entry(&digest.into(), jid.into());
            self.test_state.take();
        }

        fn manifest_read_for_job_complete(
            &mut self,
            digest: impl Into<Sha256Digest>,
            jid: impl Into<JobId>,
            result: Result<()>,
            expected: bool,
        ) {
            let actual = self
                .sut
                .manifest_read_for_job_complete(digest.into(), jid.into(), result);
            assert_eq!(actual, expected);
            self.test_state.take();
        }
    }

    #[test]
    fn start_job_get_tar_artifact_success() {
        let mut fixture = Fixture::new();
        fixture.client_connected(1);

        fixture
            .expect()
            .get_artifact((1, 2), 3, GetArtifact::Success);
        fixture.start_job((1, 2), [(3, Tar)], StartJob::Ready);
    }

    #[test]
    fn start_job_duplicate_artifacts_get_tar_artifact_success() {
        let mut fixture = Fixture::new();
        fixture.client_connected(1);

        fixture
            .expect()
            .get_artifact((1, 2), 3, GetArtifact::Success);
        fixture.start_job((1, 2), [(3, Tar), (3, Tar)], StartJob::Ready);
    }

    #[test]
    fn start_job_get_tar_artifact_get() {
        let mut fixture = Fixture::new();
        fixture.client_connected(1);

        fixture
            .expect()
            .get_artifact((1, 2), 3, GetArtifact::Get)
            .send_message_to_client(1, BrokerToClient::TransferArtifact(digest!(3)));
        fixture.start_job((1, 2), [(3, Tar)], StartJob::NotReady);

        fixture.expect().got_artifact(3, "foo.tmp", Ok([(1, 2)]));
        fixture.artifact_transferred_ok(3, "foo.tmp", [(1, 2)]);
    }

    #[test]
    fn start_job_duplicate_artifacts_get_tar_artifact_get() {
        let mut fixture = Fixture::new();
        fixture.client_connected(1);

        fixture
            .expect()
            .get_artifact((1, 2), 3, GetArtifact::Get)
            .send_message_to_client(1, BrokerToClient::TransferArtifact(digest!(3)));
        fixture.start_job((1, 2), [(3, Tar), (3, Tar)], StartJob::NotReady);

        fixture.expect().got_artifact(3, "foo.tmp", Ok([(1, 2)]));
        fixture.artifact_transferred_ok(3, "foo.tmp", [(1, 2)]);
    }

    #[test]
    fn start_job_get_tar_artifact_wait() {
        let mut fixture = Fixture::new();
        fixture.client_connected(1);

        fixture.expect().get_artifact((1, 2), 3, GetArtifact::Wait);
        fixture.start_job((1, 2), [(3, Tar)], StartJob::NotReady);
    }

    #[test]
    fn start_job_duplicate_artifacts_get_tar_artifact_wait() {
        let mut fixture = Fixture::new();
        fixture.client_connected(1);

        fixture.expect().get_artifact((1, 2), 3, GetArtifact::Wait);
        fixture.start_job((1, 2), [(3, Tar), (3, Tar)], StartJob::NotReady);
    }

    #[test]
    fn start_job_get_manifest_artifact_success() {
        let mut fixture = Fixture::new();
        fixture.client_connected(1);

        fixture
            .expect()
            .get_artifact((1, 2), 3, GetArtifact::Success)
            .read_artifact(3, 33)
            .send_message_to_manifest_reader((1, 2), 3, 33);
        fixture.start_job((1, 2), [(3, Manifest)], StartJob::NotReady);
    }

    #[test]
    fn start_job_duplicate_artifacts_get_manifest_artifact_success() {
        let mut fixture = Fixture::new();
        fixture.client_connected(1);

        fixture
            .expect()
            .get_artifact((1, 2), 3, GetArtifact::Success)
            .read_artifact(3, 33)
            .send_message_to_manifest_reader((1, 2), 3, 33);
        fixture.start_job((1, 2), [(3, Manifest), (3, Manifest)], StartJob::NotReady);
    }

    #[test]
    fn start_job_get_manifest_artifact_get() {
        let mut fixture = Fixture::new();
        fixture.client_connected(1);
        fixture
            .expect()
            .get_artifact((1, 2), 3, GetArtifact::Get)
            .send_message_to_client(1, BrokerToClient::TransferArtifact(digest!(3)));
        fixture.start_job((1, 2), [(3, Manifest)], StartJob::NotReady);
    }

    #[test]
    fn start_job_duplicate_artifacts_get_manifest_artifact_get() {
        let mut fixture = Fixture::new();
        fixture.client_connected(1);
        fixture
            .expect()
            .get_artifact((1, 2), 3, GetArtifact::Get)
            .send_message_to_client(1, BrokerToClient::TransferArtifact(digest!(3)));
        fixture.start_job((1, 2), [(3, Manifest), (3, Manifest)], StartJob::NotReady);
    }

    #[test]
    fn start_job_get_manifest_artifact_wait() {
        let mut fixture = Fixture::new();
        fixture.client_connected(1);
        fixture.expect().get_artifact((1, 2), 3, GetArtifact::Wait);
        fixture.start_job((1, 2), [(3, Manifest)], StartJob::NotReady);
    }

    #[test]
    fn start_job_duplicate_artifacts_get_manifest_artifact_wait() {
        let mut fixture = Fixture::new();
        fixture.client_connected(1);
        fixture.expect().get_artifact((1, 2), 3, GetArtifact::Wait);
        fixture.start_job((1, 2), [(3, Manifest), (3, Manifest)], StartJob::NotReady);
    }

    #[test]
    fn manifest_read_for_job_entry_from_disconnected_client() {
        let mut fixture = Fixture::new();
        fixture.client_connected(1);

        fixture
            .expect()
            .get_artifact((1, 2), 3, GetArtifact::Success)
            .read_artifact(3, 33)
            .send_message_to_manifest_reader((1, 2), 3, 33);
        fixture.start_job((1, 2), [(3, Manifest)], StartJob::NotReady);

        fixture
            .expect()
            .client_disconnected(1)
            .decrement_refcount(3);
        fixture.client_disconnected(1);

        fixture.manifest_read_for_job_entry(4, (1, 2));
    }

    #[test]
    fn manifest_read_for_job_complete_from_disconnected_client() {
        let mut fixture = Fixture::new();
        fixture.client_connected(1);

        fixture
            .expect()
            .get_artifact((1, 2), 3, GetArtifact::Success)
            .read_artifact(3, 33)
            .send_message_to_manifest_reader((1, 2), 3, 33);
        fixture.start_job((1, 2), [(3, Manifest)], StartJob::NotReady);

        fixture
            .expect()
            .client_disconnected(1)
            .decrement_refcount(3);
        fixture.client_disconnected(1);

        fixture.manifest_read_for_job_complete(3, (1, 2), Ok(()), false);
    }

    #[test]
    fn manifest_read_for_job_entry_various_cache_states() {
        let mut fixture = Fixture::new();
        fixture.client_connected(1);

        fixture
            .expect()
            .get_artifact((1, 2), 3, GetArtifact::Success)
            .read_artifact(3, 33)
            .send_message_to_manifest_reader((1, 2), 3, 33);
        fixture.start_job((1, 2), [(3, Manifest)], StartJob::NotReady);

        fixture
            .expect()
            .get_artifact((1, 2), 4, GetArtifact::Success);
        fixture.manifest_read_for_job_entry(4, (1, 2));

        fixture.manifest_read_for_job_entry(4, (1, 2));

        fixture.expect().get_artifact((1, 2), 5, GetArtifact::Wait);
        fixture.manifest_read_for_job_entry(5, (1, 2));

        fixture.manifest_read_for_job_entry(5, (1, 2));

        fixture
            .expect()
            .get_artifact((1, 2), 6, GetArtifact::Get)
            .send_message_to_client(1, BrokerToClient::TransferArtifact(digest!(6)));
        fixture.manifest_read_for_job_entry(6, (1, 2));

        fixture.manifest_read_for_job_entry(6, (1, 2));

        fixture.manifest_read_for_job_complete(3, (1, 2), Ok(()), false);

        fixture.expect().got_artifact(6, "6.tmp", Ok([(1, 2)]));
        fixture.artifact_transferred_ok(6, "6.tmp", [] as [JobId; 0]);

        fixture.expect().got_artifact(5, "5.tmp", Ok([(1, 2)]));
        fixture.artifact_transferred_ok(5, "5.tmp", [(1, 2)]);
    }

    #[test]
    fn artifact_tranferred_ok_for_multiple_jobs() {
        let mut fixture = Fixture::new();
        fixture.client_connected(1);
        fixture.client_connected(2);

        fixture
            .expect()
            .get_artifact((1, 2), 3, GetArtifact::Get)
            .send_message_to_client(1, BrokerToClient::TransferArtifact(digest!(3)));
        fixture.start_job((1, 2), [(3, Tar)], StartJob::NotReady);

        fixture.expect().get_artifact((2, 2), 3, GetArtifact::Wait);
        fixture.start_job((2, 2), [(3, Tar)], StartJob::NotReady);

        fixture
            .expect()
            .got_artifact(3, "foo.tmp", Ok([(1, 2), (2, 2)]));
        fixture.artifact_transferred_ok(3, "foo.tmp", [(1, 2), (2, 2)]);
    }

    #[test]
    fn artifact_tranferred_ok_kicks_off_manifest_read() {
        let mut fixture = Fixture::new();
        fixture.client_connected(1);

        fixture
            .expect()
            .get_artifact((1, 2), 3, GetArtifact::Get)
            .send_message_to_client(1, BrokerToClient::TransferArtifact(digest!(3)));
        fixture.start_job((1, 2), [(3, Manifest)], StartJob::NotReady);

        fixture
            .expect()
            .got_artifact(3, "foo.tmp", Ok([(1, 2)]))
            .read_artifact(3, 33)
            .send_message_to_manifest_reader((1, 2), 3, 33);
        fixture.artifact_transferred_ok(3, "foo.tmp", [] as [JobId; 0]);
    }

    #[test]
    fn manifest_read_for_job_complete_tracks_count_for_job() {
        let mut fixture = Fixture::new();
        fixture.client_connected(1);

        fixture
            .expect()
            .get_artifact((1, 2), 3, GetArtifact::Success)
            .get_artifact((1, 2), 4, GetArtifact::Success)
            .read_artifact(3, 33)
            .send_message_to_manifest_reader((1, 2), 3, 33)
            .read_artifact(4, 44)
            .send_message_to_manifest_reader((1, 2), 4, 44);
        fixture.start_job((1, 2), [(3, Manifest), (4, Manifest)], StartJob::NotReady);

        fixture.manifest_read_for_job_complete(3, (1, 2), Ok(()), false);
        fixture.manifest_read_for_job_complete(4, (1, 2), Ok(()), true);
    }

    #[test]
    fn complete_job_one_artifact() {
        let mut fixture = Fixture::new();
        fixture.client_connected(1);

        fixture
            .expect()
            .get_artifact((1, 2), 3, GetArtifact::Success);
        fixture.start_job((1, 2), [(3, Tar)], StartJob::Ready);

        fixture.expect().decrement_refcount(3);
        fixture.complete_job((1, 2));
    }

    #[test]
    fn complete_job_two_artifacts() {
        let mut fixture = Fixture::new();
        fixture.client_connected(1);

        fixture
            .expect()
            .get_artifact((1, 2), 3, GetArtifact::Success)
            .get_artifact((1, 2), 4, GetArtifact::Success);
        fixture.start_job((1, 2), [(3, Tar), (4, Tar)], StartJob::Ready);

        fixture.expect().decrement_refcount(3).decrement_refcount(4);
        fixture.complete_job((1, 2));
    }

    #[test]
    fn complete_job_duplicate_artifacts() {
        let mut fixture = Fixture::new();
        fixture.client_connected(1);

        fixture
            .expect()
            .get_artifact((1, 2), 3, GetArtifact::Success);
        fixture.start_job((1, 2), [(3, Tar), (3, Tar)], StartJob::Ready);

        fixture.expect().decrement_refcount(3);
        fixture.complete_job((1, 2));
    }

    #[test]
    fn client_disconnect_no_jobs() {
        let mut fixture = Fixture::new();
        fixture.client_connected(1);

        fixture.expect().client_disconnected(1);
        fixture.client_disconnected(1);
    }

    #[test]
    fn client_disconnect_jobs_with_some_artifacts() {
        let mut fixture = Fixture::new();
        fixture.client_connected(1);

        fixture
            .expect()
            .get_artifact((1, 1), 1, GetArtifact::Success)
            .get_artifact((1, 1), 2, GetArtifact::Wait)
            .get_artifact((1, 1), 3, GetArtifact::Get)
            .get_artifact((1, 1), 4, GetArtifact::Success)
            .read_artifact(4, 44)
            .send_message_to_manifest_reader((1, 1), 4, 44)
            .send_message_to_client(1, BrokerToClient::TransferArtifact(digest!(3)));
        fixture.start_job(
            (1, 1),
            [(1, Tar), (2, Tar), (3, Tar), (4, Manifest), (1, Tar)],
            StartJob::NotReady,
        );

        fixture
            .expect()
            .get_artifact((1, 2), 1, GetArtifact::Success);
        fixture.start_job((1, 2), [(1, Tar)], StartJob::Ready);

        fixture.client_connected(2);
        fixture
            .expect()
            .get_artifact((2, 2), 1, GetArtifact::Success);
        fixture.start_job((2, 2), [(1, Tar)], StartJob::Ready);

        fixture
            .expect()
            .client_disconnected(1)
            .decrement_refcount(1)
            .decrement_refcount(1)
            .decrement_refcount(4);
        fixture.client_disconnected(1);
    }
}
