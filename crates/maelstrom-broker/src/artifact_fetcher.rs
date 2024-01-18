use crate::scheduler_task::{SchedulerMessage, SchedulerSender};
use anyhow::Result;
use maelstrom_base::{
    proto::{
        ArtifactFetcherToBroker, ArtifactMetadata, ArtifactType, BrokerToArtifactFetcher, Identity,
        ManifestEntry, ManifestEntryData, ManifestReader,
    },
    Sha256Digest,
};
use maelstrom_util::{
    fs::{File, Fs},
    net,
};
use slog::{debug, Logger};
use std::io::Seek as _;
use std::{io, net::TcpStream, str, sync::mpsc};

fn get_file<'fs>(
    fs: &'fs Fs,
    digest: &Sha256Digest,
    scheduler_sender: &SchedulerSender,
) -> Result<(File<'fs>, ArtifactMetadata)> {
    let (channel_sender, channel_receiver) = mpsc::channel();
    scheduler_sender.send(SchedulerMessage::GetArtifactForWorker(
        digest.clone(),
        channel_sender,
    ))?;

    let (path, artifact_meta) = channel_receiver.recv()??;
    let f = fs.open_file(path)?;
    Ok((f, artifact_meta))
}

fn add_entry_to_tar(
    fs: &Fs,
    tar: &mut tar::Builder<impl io::Write>,
    scheduler_sender: &SchedulerSender,
    entry: &ManifestEntry,
) -> Result<()> {
    let mut header = tar::Header::new_gnu();
    header.set_size(entry.metadata.size);
    match &entry.metadata.user {
        Identity::Id(id) => header.set_uid(*id),
        Identity::Name(name) => header.set_username(name)?,
    }
    match &entry.metadata.group {
        Identity::Id(id) => header.set_gid(*id),
        Identity::Name(name) => header.set_groupname(name)?,
    }
    header.set_mode(entry.metadata.mode.into());
    header.set_mtime(i64::from(entry.metadata.mtime) as u64);

    match &entry.data {
        ManifestEntryData::File(Some(digest)) => {
            header.set_entry_type(tar::EntryType::Regular);
            let (mut f, artifact_meta) = get_file(fs, digest, scheduler_sender)?;
            assert_eq!(artifact_meta.type_, ArtifactType::Binary);
            tar.append_data(&mut header, &entry.path, &mut f)?;
            assert_eq!(f.stream_position()?, artifact_meta.size);
            scheduler_sender.send(SchedulerMessage::DecrementRefcount(artifact_meta.digest))?;
        }
        ManifestEntryData::File(None) => {
            header.set_entry_type(tar::EntryType::Regular);
            tar.append_data(&mut header, &entry.path, io::empty())?;
        }
        ManifestEntryData::Directory => {
            header.set_entry_type(tar::EntryType::Directory);
            tar.append_data(&mut header, &entry.path, io::empty())?;
        }
        ManifestEntryData::Symlink(data) => {
            header.set_entry_type(tar::EntryType::Symlink);
            let target = str::from_utf8(data)?;
            tar.append_link(&mut header, &entry.path, target)?;
        }
        ManifestEntryData::Hardlink(target) => {
            header.set_entry_type(tar::EntryType::Link);
            tar.append_link(&mut header, &entry.path, target)?;
        }
    }

    Ok(())
}

fn send_manifest(
    fs: &Fs,
    scheduler_sender: &SchedulerSender,
    mut file: &mut File<'_>,
    mut socket: &mut impl io::Write,
    artifact_meta: ArtifactMetadata,
) -> Result<()> {
    let mut tar = tar::Builder::new(&mut socket);
    for entry in ManifestReader::new(&mut file)? {
        add_entry_to_tar(fs, &mut tar, scheduler_sender, &entry?)?;
    }
    tar.finish()?;

    assert_eq!(file.stream_position()?, artifact_meta.size);
    scheduler_sender.send(SchedulerMessage::DecrementRefcount(artifact_meta.digest))?;

    Ok(())
}

fn send_tar(
    scheduler_sender: &SchedulerSender,
    mut file: &mut File<'_>,
    mut socket: &mut impl io::Write,
    artifact_meta: ArtifactMetadata,
) -> Result<()> {
    let copied = io::copy(&mut file, &mut socket)?;
    assert_eq!(copied, artifact_meta.size);
    scheduler_sender.send(SchedulerMessage::DecrementRefcount(artifact_meta.digest))?;
    Ok(())
}

fn handle_one_message(
    msg: ArtifactFetcherToBroker,
    mut socket: &mut impl io::Write,
    scheduler_sender: &SchedulerSender,
    log: &mut Logger,
) -> Result<()> {
    debug!(log, "received artifact fetcher message"; "msg" => ?msg);
    let ArtifactFetcherToBroker(digest) = msg;
    let fs = Fs::new();
    let result = get_file(&fs, &digest, scheduler_sender);
    let msg = BrokerToArtifactFetcher(
        result
            .as_ref()
            .map(|(_, artifact_meta)| artifact_meta.size)
            .map_err(|e| e.to_string()),
    );
    debug!(log, "sending artifact fetcher message"; "msg" => ?msg);
    net::write_message_to_socket(&mut socket, msg)?;
    let (mut f, artifact_meta) = result?;
    match artifact_meta.type_ {
        ArtifactType::Manifest => {
            send_manifest(&fs, scheduler_sender, &mut f, &mut socket, artifact_meta)?
        }
        ArtifactType::Tar => send_tar(scheduler_sender, &mut f, &mut socket, artifact_meta)?,
        ArtifactType::Binary => unreachable!(),
    }
    Ok(())
}

fn connection_loop(
    mut socket: TcpStream,
    scheduler_sender: &SchedulerSender,
    log: &mut Logger,
) -> Result<()> {
    loop {
        let msg = net::read_message_from_socket(&mut socket)?;
        handle_one_message(msg, &mut socket, scheduler_sender, log)?;
    }
}

pub fn connection_main(
    socket: TcpStream,
    scheduler_sender: SchedulerSender,
    mut log: Logger,
) -> Result<()> {
    debug!(log, "artifact fetcher connected");
    let err = connection_loop(socket, &scheduler_sender, &mut log).unwrap_err();
    debug!(log, "artifact fetcher disconnected"; "err" => %err);
    Err(err)
}

#[cfg(test)]
mod tests {
    use super::*;
    use assert_matches::assert_matches;
    use maelstrom_base::proto::{ManifestEntryMetadata, ManifestWriter, Mode, UnixTimestamp};
    use maelstrom_test::*;
    use std::io::Read as _;
    use std::os::unix::fs::MetadataExt as _;
    use std::path::Path;
    use std::thread;
    use tempfile::{tempdir, TempDir};
    use tokio::sync::mpsc::{unbounded_channel, UnboundedReceiver};

    fn write_manifest(path: &Path, entries: Vec<ManifestEntry>) -> u64 {
        let fs = Fs::new();
        let mut f = fs.create_file(path).unwrap();
        let mut writer = ManifestWriter::new(&mut f).unwrap();
        writer.write_entries(&entries).unwrap();
        f.stream_position().unwrap()
    }

    async fn send_manifest(
        tmp_dir: &TempDir,
        receiver: &mut UnboundedReceiver<SchedulerMessage>,
        manifest_entries: Vec<ManifestEntry>,
    ) {
        let SchedulerMessage::GetArtifactForWorker(digest, sender) = receiver.recv().await.unwrap()
        else {
            panic!()
        };
        let manifest_path = tmp_dir.path().join(format!("{digest}.manifest"));
        let size = write_manifest(&manifest_path, manifest_entries);
        sender
            .send(Ok((
                manifest_path,
                ArtifactMetadata {
                    digest,
                    type_: ArtifactType::Manifest,
                    size,
                },
            )))
            .unwrap();
    }

    fn put_file(path: &Path, data: &[u8]) {
        let fs = Fs::new();
        fs.write(path, data).unwrap();
    }

    async fn send_binary(
        tmp_dir: &TempDir,
        receiver: &mut UnboundedReceiver<SchedulerMessage>,
        data: &[u8],
    ) {
        let SchedulerMessage::GetArtifactForWorker(digest, sender) = receiver.recv().await.unwrap()
        else {
            panic!()
        };
        let bin_path = tmp_dir.path().join(format!("{digest}.bin"));
        put_file(&bin_path, data);
        sender
            .send(Ok((
                bin_path,
                ArtifactMetadata {
                    digest,
                    type_: ArtifactType::Binary,
                    size: data.len() as u64,
                },
            )))
            .unwrap();
    }

    async fn wait_for_ref_dec(
        receiver: &mut UnboundedReceiver<SchedulerMessage>,
        expected: Sha256Digest,
    ) {
        assert_matches!(
            receiver.recv().await.unwrap(),
            SchedulerMessage::DecrementRefcount(digest) if digest == expected
        );
    }

    fn artifact_fetcher_test<FutureT: std::future::Future>(
        broker_body: impl FnOnce(UnboundedReceiver<SchedulerMessage>) -> FutureT + Send,
        worker_body: impl FnOnce(&Path),
    ) {
        let tmp_dir = tempdir().unwrap();
        let artifact_msg = tmp_dir.path().join("sent_data.bin");

        let msg = ArtifactFetcherToBroker(digest![42]);
        let (sender, receiver) = unbounded_channel();
        let mut log = Logger::root(slog::Discard, slog::o!());
        thread::scope(|scope| {
            scope.spawn(move || {
                tokio::runtime::Builder::new_multi_thread()
                    .enable_all()
                    .build()
                    .unwrap()
                    .block_on(broker_body(receiver));
            });

            let fs = Fs::new();
            handle_one_message(
                msg,
                &mut fs.create_file(&artifact_msg).unwrap(),
                &sender,
                &mut log,
            )
            .unwrap()
        });

        let fs = Fs::new();
        let extracted_path = tmp_dir.path().join("extracted");
        let mut sent_data = fs.open_file(artifact_msg).unwrap();
        let msg: BrokerToArtifactFetcher = net::read_message_from_socket(&mut sent_data).unwrap();
        let _size = msg.0.unwrap();
        let mut tar = tar::Archive::new(sent_data);
        tar.set_preserve_mtime(true);
        tar.set_preserve_permissions(true);
        tar.unpack(&extracted_path).unwrap();
        worker_body(&extracted_path);
    }

    fn assert_file(path: &Path, data: &[u8], mode: u32, mtime: i64) {
        let fs = Fs::new();

        let mut f = fs.open_file(path).unwrap();

        let mut contents = Vec::new();
        f.read_to_end(&mut contents).unwrap();
        assert_eq!(contents, data);

        let meta = f.metadata().unwrap();
        assert_eq!(meta.len(), data.len() as u64);
        assert_eq!(meta.mode(), mode);
        assert_eq!(meta.mtime(), mtime);
        assert!(meta.file_type().is_file());
    }

    fn assert_dir(path: &Path, mode: u32) {
        let fs = Fs::new();

        let f = fs.open_file(path).unwrap();

        let meta = f.metadata().unwrap();
        assert_eq!(meta.mode(), mode);
        assert!(meta.file_type().is_dir());
    }

    #[test]
    fn manifest_to_tar_two_files() {
        artifact_fetcher_test(
            |mut receiver| async move {
                let tmp_dir = tempdir().unwrap();
                send_manifest(
                    &tmp_dir,
                    &mut receiver,
                    vec![
                        ManifestEntry {
                            path: "foobar.txt".into(),
                            metadata: ManifestEntryMetadata {
                                size: 11,
                                mode: Mode(0o0555),
                                // The user and group won't take effect because we don't have
                                // permission
                                user: Identity::Id(1001),
                                group: Identity::Id(1002),
                                mtime: UnixTimestamp(1705538554),
                            },
                            data: ManifestEntryData::File(Some(digest![43])),
                        },
                        ManifestEntry {
                            path: "empty_file".into(),
                            metadata: ManifestEntryMetadata {
                                size: 0,
                                mode: Mode(0o0555),
                                // The user and group won't take effect because we don't have
                                // permission
                                user: Identity::Id(1001),
                                group: Identity::Id(1002),
                                mtime: UnixTimestamp(1705538554),
                            },
                            data: ManifestEntryData::File(None),
                        },
                    ],
                )
                .await;
                send_binary(&tmp_dir, &mut receiver, b"hello world").await;
                wait_for_ref_dec(&mut receiver, digest![43]).await;
                wait_for_ref_dec(&mut receiver, digest![42]).await;
            },
            |extracted_path| {
                assert_file(
                    &extracted_path.join("foobar.txt"),
                    b"hello world",
                    0o100555,
                    1705538554,
                );

                assert_file(
                    &extracted_path.join("empty_file"),
                    b"",
                    0o100555,
                    1705538554,
                );
            },
        );
    }

    #[test]
    fn manifest_to_tar_directories_and_links() {
        artifact_fetcher_test(
            |mut receiver| async move {
                let tmp_dir = tempdir().unwrap();
                send_manifest(
                    &tmp_dir,
                    &mut receiver,
                    vec![
                        ManifestEntry {
                            path: "foobar/a_file".into(),
                            metadata: ManifestEntryMetadata {
                                size: 0,
                                mode: Mode(0o0555),
                                // The user and group won't take effect because we don't have
                                // permission
                                user: Identity::Id(1001),
                                group: Identity::Id(1002),
                                mtime: UnixTimestamp(1705538554),
                            },
                            data: ManifestEntryData::File(None),
                        },
                        ManifestEntry {
                            path: "foobar/a_symlink".into(),
                            metadata: ManifestEntryMetadata {
                                size: 0,
                                mode: Mode(0o0555),
                                // The user and group won't take effect because we don't have
                                // permission
                                user: Identity::Id(1001),
                                group: Identity::Id(1002),
                                mtime: UnixTimestamp(1705538554),
                            },
                            data: ManifestEntryData::Symlink(b"./a_file".into()),
                        },
                        ManifestEntry {
                            path: "foobar/a_hardlink".into(),
                            metadata: ManifestEntryMetadata {
                                size: 0,
                                mode: Mode(0o0555),
                                // The user and group won't take effect because we don't have
                                // permission
                                user: Identity::Id(1001),
                                group: Identity::Id(1002),
                                mtime: UnixTimestamp(1705538554),
                            },
                            data: ManifestEntryData::Hardlink("foobar/a_file".into()),
                        },
                        ManifestEntry {
                            path: "foobar".into(),
                            metadata: ManifestEntryMetadata {
                                size: 0,
                                mode: Mode(0o0555),
                                // The user and group won't take effect because we don't have
                                // permission
                                user: Identity::Id(1001),
                                group: Identity::Id(1002),
                                mtime: UnixTimestamp(1705538554),
                            },
                            data: ManifestEntryData::Directory,
                        },
                        ManifestEntry {
                            path: "baz".into(),
                            metadata: ManifestEntryMetadata {
                                size: 0,
                                mode: Mode(0o0555),
                                // The user and group won't take effect because we don't have
                                // permission
                                user: Identity::Id(1001),
                                group: Identity::Id(1002),
                                mtime: UnixTimestamp(1705538554),
                            },
                            data: ManifestEntryData::Directory,
                        },
                    ],
                )
                .await;
                wait_for_ref_dec(&mut receiver, digest![42]).await;
            },
            |extracted_path| {
                let a_file = extracted_path.join("foobar/a_file");
                let hardlink = &extracted_path.join("foobar/a_hardlink");
                assert_file(&a_file, b"", 0o100555, 1705538554);
                assert_file(&hardlink, b"", 0o100555, 1705538554);

                let fs = Fs::new();
                assert_eq!(
                    fs.metadata(a_file).unwrap().ino(),
                    fs.metadata(hardlink).unwrap().ino()
                );

                assert_dir(&extracted_path.join("baz"), 0o40555);
                assert_dir(&extracted_path.join("foobar"), 0o40555);
            },
        );
    }
}
