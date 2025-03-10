mod job_output;
pub mod main_app;
pub mod watch;

#[cfg(test)]
mod tests;

use crate::{
    config::{Repeat, StopAfter},
    deps::{KillOnDrop, TestArtifact as _, TestCollector, Wait as _, WaitStatus},
    metadata::Store as MetadataStore,
    test_db::TestDb,
    ui::{UiJobId as JobId, UiMessage, UiSender},
    util::{ListTests, UseColor},
};
use anyhow::Result;
use maelstrom_base::Timeout;
use maelstrom_client::{spec::JobSpec, Client, JobStatus};
use std::sync::{mpsc::Sender, Arc, Mutex};
use std_semaphore::Semaphore;

type ArtifactM<DepsT> = <<DepsT as Deps>::TestCollector as TestCollector>::Artifact;
type ArtifactKeyM<DepsT> = <<DepsT as Deps>::TestCollector as TestCollector>::ArtifactKey;
type CaseMetadataM<DepsT> = <<DepsT as Deps>::TestCollector as TestCollector>::CaseMetadata;
type PackageM<DepsT> = <<DepsT as Deps>::TestCollector as TestCollector>::Package;
type PackageIdM<DepsT> = <<DepsT as Deps>::TestCollector as TestCollector>::PackageId;
type TestFilterM<DepsT> = <<DepsT as Deps>::TestCollector as TestCollector>::TestFilter;
pub type TestDbM<DepsT> = TestDb<ArtifactKeyM<DepsT>, CaseMetadataM<DepsT>>;
type TestingOptionsM<DepsT> = TestingOptions<TestFilterM<DepsT>>;

pub trait Deps {
    type TestCollector: TestCollector + Sync;

    fn start_collection(&self, use_color: UseColor, packages: Vec<&PackageM<Self>>);
    fn get_packages(&self);
    fn add_job(&self, job_id: JobId, spec: JobSpec);
    fn list_tests(&self, artifact: ArtifactM<Self>);
    fn start_shutdown(&self);
    fn send_ui_msg(&self, msg: UiMessage);
}

/// Immutable information used to control the testing invocation.
pub struct TestingOptions<TestFilterT> {
    pub test_metadata: MetadataStore<TestFilterT>,
    pub filter: TestFilterT,
    pub timeout_override: Option<Option<Timeout>>,
    pub use_color: UseColor,
    pub repeat: Repeat,
    pub stop_after: Option<StopAfter>,
    pub list_tests: ListTests,
}

pub enum MainAppMessage<PackageT: 'static, ArtifactT: 'static, CaseMetadataT: 'static> {
    Start,
    Packages {
        packages: Vec<PackageT>,
    },
    ArtifactBuilt {
        artifact: ArtifactT,
    },
    TestsListed {
        artifact: ArtifactT,
        listing: Vec<(String, CaseMetadataT)>,
        ignored_listing: Vec<String>,
    },
    FatalError {
        error: anyhow::Error,
    },
    JobUpdate {
        job_id: JobId,
        result: Result<JobStatus>,
    },
    CollectionFinished {
        wait_status: WaitStatus,
    },
}

pub type MainAppMessageM<DepsT> =
    MainAppMessage<PackageM<DepsT>, ArtifactM<DepsT>, CaseMetadataM<DepsT>>;

pub enum ControlMessage<MessageT> {
    Shutdown,
    App { msg: MessageT },
}

impl<MessageT> From<MessageT> for ControlMessage<MessageT> {
    fn from(msg: MessageT) -> Self {
        Self::App { msg }
    }
}

type WaitM<DepsT> = <<DepsT as Deps>::TestCollector as TestCollector>::BuildHandle;

pub struct MainAppDepsAdapter<'deps, 'scope, TestCollectorT: TestCollector + Sync> {
    test_collector: &'deps TestCollectorT,
    scope: &'scope std::thread::Scope<'scope, 'deps>,
    main_app_sender: Sender<ControlMessage<MainAppMessageM<Self>>>,
    ui: UiSender,
    collect_killer: Mutex<Option<KillOnDrop<WaitM<Self>>>>,
    semaphore: &'deps Semaphore,
    client: &'deps Client,
}

impl<'deps, 'scope, TestCollectorT: TestCollector + Sync>
    MainAppDepsAdapter<'deps, 'scope, TestCollectorT>
{
    pub fn new(
        test_collector: &'deps TestCollectorT,
        scope: &'scope std::thread::Scope<'scope, 'deps>,
        main_app_sender: Sender<ControlMessage<MainAppMessageM<Self>>>,
        ui: UiSender,
        semaphore: &'deps Semaphore,
        client: &'deps Client,
    ) -> Self {
        Self {
            test_collector,
            scope,
            main_app_sender,
            ui,
            collect_killer: Mutex::new(None),
            semaphore,
            client,
        }
    }
}

impl<TestCollectorT: TestCollector + Sync> Deps for MainAppDepsAdapter<'_, '_, TestCollectorT> {
    type TestCollector = TestCollectorT;

    fn start_collection(&self, use_color: UseColor, packages: Vec<&PackageM<Self>>) {
        let sem = self.semaphore;
        let sender = self.main_app_sender.clone();
        match self.test_collector.start(use_color, packages, &self.ui) {
            Ok((build_handle, artifact_stream)) => {
                let build_handle = Arc::new(build_handle);
                let killer = KillOnDrop::new(build_handle.clone());
                let _ = self.collect_killer.lock().unwrap().replace(killer);

                self.scope.spawn(move || {
                    let _guard = sem.access();
                    for artifact in artifact_stream {
                        match artifact {
                            Ok(artifact) => {
                                if sender
                                    .send(MainAppMessage::ArtifactBuilt { artifact }.into())
                                    .is_err()
                                {
                                    break;
                                }
                            }
                            Err(error) => {
                                let _ = sender.send(MainAppMessage::FatalError { error }.into());
                                break;
                            }
                        }
                    }
                    match build_handle.wait() {
                        Ok(wait_status) => {
                            let _ = sender
                                .send(MainAppMessage::CollectionFinished { wait_status }.into());
                        }
                        Err(error) => {
                            let _ = sender.send(MainAppMessage::FatalError { error }.into());
                        }
                    }
                });
            }
            Err(error) => {
                let _ = sender.send(MainAppMessage::FatalError { error }.into());
            }
        }
    }

    fn get_packages(&self) {
        let sem = self.semaphore;
        let test_collector = self.test_collector;
        let sender = self.main_app_sender.clone();
        let ui = self.ui.clone();
        self.scope.spawn(move || {
            let _guard = sem.access();
            match test_collector.get_packages(&ui) {
                Ok(packages) => {
                    let _ = sender.send(MainAppMessage::Packages { packages }.into());
                }
                Err(error) => {
                    let _ = sender.send(MainAppMessage::FatalError { error }.into());
                }
            }
        });
    }

    fn add_job(&self, job_id: JobId, spec: JobSpec) {
        let sender = self.main_app_sender.clone();

        let cb_sender = sender.clone();
        let res = self.client.add_job(spec, move |result| {
            let _ = cb_sender.send(MainAppMessage::JobUpdate { job_id, result }.into());
        });
        if let Err(error) = res {
            let _ = sender.send(MainAppMessage::FatalError { error }.into());
        }
    }

    fn list_tests(&self, artifact: ArtifactM<Self>) {
        let sem = self.semaphore;
        let sender = self.main_app_sender.clone();
        self.scope.spawn(move || {
            let _guard = sem.access();
            let listing = match artifact.list_tests() {
                Ok(listing) => listing,
                Err(error) => {
                    let _ = sender.send(MainAppMessage::FatalError { error }.into());
                    return;
                }
            };
            let ignored_listing = match artifact.list_ignored_tests() {
                Ok(listing) => listing,
                Err(error) => {
                    let _ = sender.send(MainAppMessage::FatalError { error }.into());
                    return;
                }
            };

            let _ = sender.send(
                MainAppMessage::TestsListed {
                    artifact,
                    listing,
                    ignored_listing,
                }
                .into(),
            );
        });
    }

    fn start_shutdown(&self) {
        let _ = self.main_app_sender.send(ControlMessage::Shutdown);
    }

    fn send_ui_msg(&self, msg: UiMessage) {
        self.ui.send(msg);
    }
}
