use maelstrom_base::{
    manifest::{ManifestEntryData, ManifestReader},
    ArtifactType, JobOutputResult, JobSpec, JobStatus, JobSuccess, Layer, PrefixOptions,
    Sha256Digest,
};
use maelstrom_client::Client;
use maelstrom_test::{
    client_driver::TestClientDriver,
    fake_broker::{FakeBroker, FakeBrokerJobAction, FakeBrokerState, JobSpecMatcher},
    utf8_path_buf,
};
use maelstrom_util::fs::Fs;
use maplit::hashmap;
use nonempty::{nonempty, NonEmpty};
use sha2::{Digest as _, Sha256};
use std::path::{Path, PathBuf};
use std::sync::mpsc;
use tempfile::tempdir;

fn basic_job_test(
    add_artifacts: impl FnOnce(&mut Client, &Path) -> NonEmpty<(Sha256Digest, ArtifactType)>,
    verify_artifacts: impl FnOnce(&Path, &NonEmpty<(Sha256Digest, ArtifactType)>),
) {
    let fs = Fs::new();
    let tmp_dir = tempdir().unwrap();
    let cache_dir = tmp_dir.path().join("cache");
    fs.create_dir(&cache_dir).unwrap();
    let artifact_dir = tmp_dir.path().join("artifacts");
    fs.create_dir(&artifact_dir).unwrap();

    let test_job_result = JobSuccess {
        status: JobStatus::Exited(0),
        stdout: JobOutputResult::None,
        stderr: JobOutputResult::Inline(Box::new(*b"this output should be ignored")),
    };

    let state = FakeBrokerState {
        job_responses: hashmap! {
            JobSpecMatcher {
                binary: "foo".into(),
                first_arg: "bar".into(),
            } => FakeBrokerJobAction::Respond(Ok(test_job_result.clone())),
        },
        ..Default::default()
    };
    let mut broker = FakeBroker::new(state);
    let client_driver = TestClientDriver::default();
    let mut client = Client::new(
        client_driver.clone(),
        broker.address().clone(),
        &artifact_dir,
        cache_dir,
    )
    .unwrap();
    let mut broker_conn = broker.accept();

    let layers = add_artifacts(&mut client, &artifact_dir);
    client_driver.process_client_messages();

    let spec = JobSpec {
        program: utf8_path_buf!("foo"),
        arguments: vec!["bar".into()],
        environment: vec![],
        layers: layers.clone(),
        devices: Default::default(),
        mounts: vec![],
        enable_loopback: false,
        enable_writable_file_system: false,
        working_directory: utf8_path_buf!("."),
        user: 1000.into(),
        group: 1000.into(),
    };
    let (send, recv) = mpsc::channel();
    client.add_job(
        spec,
        Box::new(move |id, result| Ok(send.send((id, result)).unwrap())),
    );

    client_driver.process_client_messages();
    broker_conn.process(1, true /* fetch_layers */);

    for _ in 0..layers.len() {
        client_driver.process_broker_msg(1);
        client_driver.process_artifact(|| broker.receive_artifact(&artifact_dir));
    }

    verify_artifacts(&artifact_dir, &layers);

    broker_conn.process(1, true /* fetch_layers */);
    client_driver.process_broker_msg(1);

    let (_id, result) = recv.recv().unwrap();
    assert_eq!(test_job_result, result.unwrap());
}

#[test]
fn basic_job_with_add_artifact_tar() {
    let fs = Fs::new();
    basic_job_test(
        |client, artifact_dir| {
            let test_artifact = artifact_dir.join("test_artifact");
            fs.write(&test_artifact, b"hello world").unwrap();
            let digest = client.add_artifact(&test_artifact).unwrap();
            nonempty![(digest.clone(), ArtifactType::Tar)]
        },
        |artifact_dir, layers| {
            let digest = &layers[0].0;
            assert_eq!(
                fs.read_to_string(&artifact_dir.join(digest.to_string()))
                    .unwrap(),
                "hello world"
            );
        },
    );
}

#[test]
fn basic_job_with_add_layer_tar() {
    let fs = Fs::new();
    basic_job_test(
        |client, artifact_dir| {
            let test_artifact = artifact_dir.join("test_artifact");
            fs.write(&test_artifact, b"hello world").unwrap();

            let digest_and_type = client
                .add_layer(Layer::Tar {
                    path: test_artifact.try_into().unwrap(),
                })
                .unwrap();
            nonempty![digest_and_type]
        },
        |artifact_dir, layers| {
            let digest = &layers[0].0;
            assert_eq!(
                fs.read_to_string(&artifact_dir.join(digest.to_string()))
                    .unwrap(),
                "hello world"
            );
        },
    );
}

fn hash_data(data: &[u8]) -> Sha256Digest {
    let mut hasher = Sha256::new();
    hasher.update(data);
    Sha256Digest::new(hasher.finalize().into())
}

fn verify_single_entry_manifest(
    manifest_path: &Path,
    expected_entry_path: &Path,
    expected_entry_data: ManifestEntryData,
) {
    let fs = Fs::new();
    let entry_iter = ManifestReader::new(fs.open_file(manifest_path).unwrap()).unwrap();
    let entries = Vec::from_iter(entry_iter.map(|e| e.unwrap()));
    assert_eq!(entries.len(), 1, "{entries:?}");
    let entry = &entries[0];

    assert_eq!(expected_entry_data, entry.data);
    assert_eq!(entry.path, expected_entry_path);
}

#[test]
fn basic_job_with_add_layer_paths() {
    basic_job_test(
        |client, artifact_dir| {
            let test_artifact = artifact_dir.join("test_artifact");
            let fs = Fs::new();
            fs.write(&test_artifact, b"hello world").unwrap();

            let digest_and_type = client
                .add_layer(Layer::Paths {
                    paths: vec![test_artifact.try_into().unwrap()],
                    prefix_options: Default::default(),
                })
                .unwrap();
            nonempty![digest_and_type]
        },
        |artifact_dir, layers| {
            let digest = &layers[0].0;
            verify_single_entry_manifest(
                &artifact_dir.join(digest.to_string()),
                &artifact_dir.join("test_artifact"),
                ManifestEntryData::File(Some(hash_data(b"hello world"))),
            )
        },
    );
}

fn basic_job_with_add_layer_paths_and_prefix(
    prefix_options_factory: impl FnOnce(&Path) -> PrefixOptions,
    expected_path_factory: impl FnOnce(&Path) -> PathBuf,
) {
    basic_job_test(
        |client, artifact_dir| {
            let test_artifact = artifact_dir.join("test_artifact");
            let fs = Fs::new();
            fs.write(&test_artifact, b"hello world").unwrap();

            let digest_and_type = client
                .add_layer(Layer::Paths {
                    paths: vec![test_artifact.try_into().unwrap()],
                    prefix_options: prefix_options_factory(artifact_dir),
                })
                .unwrap();
            nonempty![digest_and_type]
        },
        |artifact_dir, layers| {
            let digest = &layers[0].0;
            verify_single_entry_manifest(
                &artifact_dir.join(digest.to_string()),
                &expected_path_factory(&artifact_dir.join("test_artifact")),
                ManifestEntryData::File(Some(hash_data(b"hello world"))),
            )
        },
    );
}

#[test]
fn prefix_options_both() {
    basic_job_with_add_layer_paths_and_prefix(
        |artifact_dir| PrefixOptions {
            strip_prefix: Some(artifact_dir.to_owned().try_into().unwrap()),
            prepend_prefix: Some("foo/".into()),
        },
        |artifact_path| Path::new("foo/").join(artifact_path.file_name().unwrap()),
    )
}

#[test]
fn prefix_options_strip_not_found() {
    basic_job_with_add_layer_paths_and_prefix(
        |_| PrefixOptions {
            strip_prefix: Some("not_there/".into()),
            prepend_prefix: None,
        },
        |artifact_path| artifact_path.to_owned(),
    )
}

#[test]
fn prefix_options_prepend_absolute() {
    basic_job_with_add_layer_paths_and_prefix(
        |_| PrefixOptions {
            strip_prefix: None,
            prepend_prefix: Some("foo/bar".into()),
        },
        |artifact_path| Path::new("foo/bar").join(artifact_path.strip_prefix("/").unwrap()),
    )
}
