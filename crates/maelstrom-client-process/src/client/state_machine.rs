use crate::{
    artifact_pusher::{self, ArtifactUploadTracker},
    digest_repo::DigestRepository,
    dispatcher, local_broker, SocketReader,
};
use anyhow::{anyhow, bail, Context as _, Error, Result};
use async_trait::async_trait;
use futures::StreamExt;
use itertools::Itertools as _;
use maelstrom_base::{
    manifest::{ManifestEntry, ManifestEntryData, ManifestEntryMetadata, Mode, UnixTimestamp},
    proto::Hello,
    stats::JobStateCounts,
    ArtifactType, ClientJobId, JobOutcomeResult, JobSpec, Sha256Digest, Utf8Path, Utf8PathBuf,
};
use maelstrom_client_base::{
    spec::{Layer, PrefixOptions, SymlinkSpec},
    ArtifactUploadProgress, ContainerImageProgress, MANIFEST_DIR, STUB_MANIFEST_DIR,
    SYMLINK_MANIFEST_DIR,
};
use maelstrom_container::{ContainerImage, ContainerImageDepot, NullProgressTracker};
use maelstrom_util::{
    async_fs,
    config::common::BrokerAddr,
    manifest::{AsyncManifestWriter, ManifestBuilder},
    net,
};
use sha2::{Digest as _, Sha256};
use slog::{debug, Drain as _};
use std::{
    cell::UnsafeCell,
    collections::{HashMap, HashSet},
    fmt,
    path::{Path, PathBuf},
    pin::pin,
};
use tokio::{
    net::TcpStream,
    sync::{mpsc, Mutex},
    task::{self, JoinSet},
};

#[derive(Clone, Copy, Debug, PartialEq)]
enum ClientState {
    NotYetStarted,
    Starting,
    Started,
    Failed,
}

pub struct ClientStateKeeper<T> {
    not_yet_started: UnsafeCell<Option<slog::Logger>>,
    started: UnsafeCell<Option<T>>,
    failed: UnsafeCell<Option<String>>,
    sender: tokio::sync::watch::Sender<ClientState>,
}

unsafe impl<T> Sync for ClientStateKeeper<T> {}

impl<T> ClientStateKeeper<T> {
    pub fn new(log: Option<slog::Logger>) -> Self {
        Self {
            not_yet_started: UnsafeCell::new(log),
            started: UnsafeCell::new(None),
            failed: UnsafeCell::new(None),
            sender: tokio::sync::watch::Sender::new(ClientState::NotYetStarted),
        }
    }

    pub fn try_to_move_to_starting(&self) -> Result<Option<slog::Logger>> {
        if !self.sender.send_if_modified(|state| {
            if *state == ClientState::NotYetStarted {
                *state = ClientState::Starting;
                true
            } else {
                false
            }
        }) {
            bail!("client already started");
        }
        let log_ptr = self.not_yet_started.get();
        Ok(unsafe { &mut *log_ptr }.take())
    }

    pub fn move_to_started(&self, state: T) {
        let ptr = self.started.get();
        unsafe { *ptr = Some(state) };
        let old = self.sender.send_replace(ClientState::Started);
        assert_eq!(old, ClientState::Starting);
    }

    pub fn fail_to_start(&self, err: String) {
        let ptr = self.failed.get();
        unsafe { *ptr = Some(err) };
        let old = self.sender.send_replace(ClientState::Failed);
        assert_eq!(old, ClientState::Starting);
    }

    pub fn started(&self) -> Result<&T> {
        match *self.sender.borrow() {
            ClientState::NotYetStarted | ClientState::Starting => {
                Err(anyhow!("client not yet started"))
            }
            ClientState::Started => {
                let started_ptr = self.started.get();
                let state = unsafe { &*started_ptr }.as_ref().unwrap();
                Ok(state)
            }
            ClientState::Failed => Err(self._done_err()),
        }
    }

    pub fn started_with_watcher(&self) -> Result<(&T, StateWatcher<'_, T>)> {
        let receiver = self.sender.subscribe();
        let idx = *receiver.borrow();
        match idx {
            ClientState::NotYetStarted | ClientState::Starting => {
                Err(anyhow!("client not yet started"))
            }
            ClientState::Started => {
                let started_ptr = self.started.get();
                let state = unsafe { &*started_ptr }.as_ref().unwrap();
                Ok((
                    state,
                    StateWatcher {
                        keeper: self,
                        receiver,
                    },
                ))
            }
            ClientState::Failed => Err(self._done_err()),
        }
    }

    fn _done_err(&self) -> Error {
        let done_ptr = self.failed.get();
        let err = unsafe { &*done_ptr }.as_ref().unwrap();
        anyhow!("client failed with error: {err}")
    }
}

pub struct StateWatcher<'a, T> {
    keeper: &'a ClientStateKeeper<T>,
    receiver: tokio::sync::watch::Receiver<ClientState>,
}

impl<'a, T> StateWatcher<'a, T> {
    pub async fn recv<U>(mut self, receiver: tokio::sync::oneshot::Receiver<U>) -> Result<U> {
        tokio::select! {
            Ok(result) = receiver => {
                Ok(result)
            }
            _ = self.receiver.changed() => {
                assert_eq!(*self.receiver.borrow(), ClientState::Failed);
                Err(self.keeper._done_err())
            }
        }
    }
}
