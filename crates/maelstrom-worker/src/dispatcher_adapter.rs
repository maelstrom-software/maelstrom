use crate::{
    dispatcher::{Deps, Message},
    executor::{self, Executor, MountDir, TmpfsDir},
    layer_fs,
    types::{DispatcherSender, TempFileFactory},
    ManifestDigestCache, MAX_IN_FLIGHT_LAYERS_BUILDS,
};
use anyhow::Result;
use maelstrom_base::{ArtifactType, JobError, JobId, JobSpec, Sha256Digest};
use maelstrom_layer_fs::{BlobDir, LayerFs, ReaderCache};
use maelstrom_util::{
    cache::GotArtifact,
    config::common::InlineLimit,
    fs::Fs,
    root::RootBuf,
    sync::{self, EventReceiver, EventSender},
    time::SystemMonotonicClock,
};
use slog::{debug, o, Logger};
use std::{
    sync::Arc,
    {path::PathBuf, time::Duration},
};
use tokio::{
    task::{self, JoinHandle},
    time,
};

const MANIFEST_DIGEST_CACHE_SIZE: usize = 10_000;

pub struct DispatcherAdapter {
    dispatcher_sender: DispatcherSender,
    inline_limit: InlineLimit,
    log: Logger,
    executor: Arc<Executor<'static, SystemMonotonicClock>>,
    blob_dir: RootBuf<BlobDir>,
    layer_fs_cache: Arc<tokio::sync::Mutex<ReaderCache>>,
    manifest_digest_cache: ManifestDigestCache,
    layer_building_semaphore: Arc<tokio::sync::Semaphore>,
    temp_file_factory: TempFileFactory,
}

impl DispatcherAdapter {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        dispatcher_sender: DispatcherSender,
        inline_limit: InlineLimit,
        log: Logger,
        mount_dir: RootBuf<MountDir>,
        tmpfs_dir: RootBuf<TmpfsDir>,
        blob_dir: RootBuf<BlobDir>,
        temp_file_factory: TempFileFactory,
    ) -> Result<Self> {
        let fs = Fs::new();
        fs.create_dir_all(&mount_dir)?;
        fs.create_dir_all(&tmpfs_dir)?;
        Ok(DispatcherAdapter {
            inline_limit,
            executor: Arc::new(Executor::new(mount_dir, tmpfs_dir, &SystemMonotonicClock)?),
            blob_dir,
            layer_fs_cache: Arc::new(tokio::sync::Mutex::new(ReaderCache::new())),
            manifest_digest_cache: ManifestDigestCache::new(
                dispatcher_sender.clone(),
                log.clone(),
                MANIFEST_DIGEST_CACHE_SIZE.try_into().unwrap(),
            ),
            dispatcher_sender,
            log,
            layer_building_semaphore: Arc::new(tokio::sync::Semaphore::new(
                MAX_IN_FLIGHT_LAYERS_BUILDS,
            )),
            temp_file_factory,
        })
    }

    fn start_job_inner(
        &mut self,
        jid: JobId,
        spec: JobSpec,
        layer_fs_path: PathBuf,
        kill_event_receiver: EventReceiver,
    ) -> Result<()> {
        debug!(self.log, "job starting"; "spec" => ?spec);
        let log = self.log.new(o!(
            "jid" => format!("{jid:?}"),
            "program" => format!("{:?}", spec.program),
            "args" => format!("{:?}", spec.arguments)
        ));

        let layer_fs = LayerFs::from_path(&layer_fs_path, self.blob_dir.as_root())?;
        let layer_fs_cache = self.layer_fs_cache.clone();
        let fuse_spawn = move |fd| {
            tokio::spawn(async move {
                if let Err(e) = layer_fs.run_fuse(log.clone(), layer_fs_cache, fd).await {
                    slog::error!(log, "FUSE handling got error {e:?}");
                }
            });
        };

        let executor = self.executor.clone();
        let spec = executor::JobSpec::from_spec(spec);
        let inline_limit = self.inline_limit;
        let dispatcher_sender = self.dispatcher_sender.clone();
        let runtime = tokio::runtime::Handle::current();
        task::spawn_blocking(move || {
            dispatcher_sender
                .send(Message::JobCompleted(
                    jid,
                    executor
                        .run_job(
                            &spec,
                            inline_limit,
                            kill_event_receiver,
                            fuse_spawn,
                            runtime,
                        )
                        .map_err(|e| e.map(|inner| inner.to_string())),
                ))
                .ok()
        });
        Ok(())
    }
}

pub struct TimerHandle(JoinHandle<()>);

impl Drop for TimerHandle {
    fn drop(&mut self) {
        self.0.abort();
    }
}

impl Deps for DispatcherAdapter {
    type JobHandle = EventSender;

    fn start_job(&mut self, jid: JobId, spec: JobSpec, layer_fs_path: PathBuf) -> Self::JobHandle {
        let (kill_event_sender, kill_event_receiver) = sync::event();
        if let Err(e) = self.start_job_inner(jid, spec, layer_fs_path, kill_event_receiver) {
            let _ = self.dispatcher_sender.send(Message::JobCompleted(
                jid,
                Err(JobError::System(e.to_string())),
            ));
        }
        kill_event_sender
    }

    type TimerHandle = TimerHandle;

    fn start_timer(&mut self, jid: JobId, duration: Duration) -> Self::TimerHandle {
        let sender = self.dispatcher_sender.clone();
        TimerHandle(task::spawn(async move {
            time::sleep(duration).await;
            sender.send(Message::JobTimer(jid)).ok();
        }))
    }

    fn build_bottom_fs_layer(
        &mut self,
        digest: Sha256Digest,
        artifact_type: ArtifactType,
        artifact_path: PathBuf,
    ) {
        let sender = self.dispatcher_sender.clone();
        let log = self.log.clone();
        let blob_dir = self.blob_dir.clone();
        let sem = self.layer_building_semaphore.clone();
        let temp_file_factory = self.temp_file_factory.clone();
        task::spawn(async move {
            let _permit = sem.acquire().await.unwrap();

            debug!(log, "building bottom FS layer");
            let result = layer_fs::build_bottom_layer(
                log.clone(),
                temp_file_factory,
                blob_dir.as_root(),
                digest.clone(),
                artifact_type,
                artifact_path,
            )
            .await;
            debug!(log, "built bottom FS layer"; "result" => ?result);
            sender
                .send(Message::BuiltBottomFsLayer(
                    digest,
                    result.map(|(source, size)| GotArtifact::Directory { source, size }),
                ))
                .ok();
        });
    }

    fn build_upper_fs_layer(
        &mut self,
        digest: Sha256Digest,
        lower_layer_path: PathBuf,
        upper_layer_path: PathBuf,
    ) {
        let sender = self.dispatcher_sender.clone();
        let log = self.log.clone();
        let blob_dir = self.blob_dir.clone();
        let sem = self.layer_building_semaphore.clone();
        let temp_file_factory = self.temp_file_factory.clone();
        task::spawn(async move {
            let _permit = sem.acquire().await.unwrap();

            debug!(log, "building upper FS layer");
            let result = layer_fs::build_upper_layer(
                log.clone(),
                temp_file_factory,
                blob_dir.as_root(),
                lower_layer_path,
                upper_layer_path,
            )
            .await;
            debug!(log, "built upper FS layer"; "result" => ?result);
            sender
                .send(Message::BuiltUpperFsLayer(
                    digest,
                    result.map(|(source, size)| GotArtifact::Directory { source, size }),
                ))
                .ok();
        });
    }

    fn read_manifest_digests(&mut self, digest: Sha256Digest, path: PathBuf, jid: JobId) {
        self.manifest_digest_cache.get(digest, path, jid);
    }
}
