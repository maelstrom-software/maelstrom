use maelstrom_base::{
    manifest::{ManifestEntryData, ManifestReader},
    ArtifactType, JobOutputResult, JobSpec, JobStatus, JobSuccess, Sha256Digest, Utf8Path,
};
use maelstrom_client::{
    spec::{Layer, PrefixOptions, SymlinkSpec},
    test::{
        client_driver::TestClientDriver,
        fake_broker::{FakeBroker, FakeBrokerJobAction, FakeBrokerState, JobSpecMatcher},
    },
    Client,
};
use maelstrom_test::utf8_path_buf;
use maelstrom_util::fs::Fs;
use maplit::hashmap;
use nonempty::{nonempty, NonEmpty};
use sha2::{Digest as _, Sha256};
use std::collections::HashMap;
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

fn verify_manifest(manifest_path: &Path, expected: Vec<(&Utf8Path, ManifestEntryData)>) {
    let fs = Fs::new();
    let entry_iter = ManifestReader::new(fs.open_file(manifest_path).unwrap()).unwrap();
    let actual = Vec::from_iter(entry_iter.map(|e| e.unwrap()));
    assert_eq!(
        actual.len(),
        expected.len(),
        "expected = {expected:?}, actual = {actual:?}"
    );

    let actual_map: HashMap<_, _> = actual.iter().map(|e| (e.path.as_path(), &e.data)).collect();
    for (path, expected_data) in expected {
        let data = actual_map
            .get(path)
            .expect(&format!("{path:?} not found in {actual_map:?}"));
        assert_eq!(*data, &expected_data);
    }
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

fn verify_empty_manifest(manifest_path: &Path) {
    let fs = Fs::new();
    let entry_iter = ManifestReader::new(fs.open_file(manifest_path).unwrap()).unwrap();
    let entries = Vec::from_iter(entry_iter.map(|e| e.unwrap()));
    assert_eq!(entries, vec![]);
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
    input_path_factory: impl FnOnce(&Path) -> PathBuf,
    prefix_options_factory: impl FnOnce(&Path) -> PrefixOptions,
    expected_path_factory: impl FnOnce(&Path) -> PathBuf,
) {
    basic_job_test(
        |client, artifact_dir| {
            let input_path = input_path_factory(&artifact_dir);
            let mut artifact_path = input_path.clone();
            if artifact_path.is_relative() {
                artifact_path = artifact_dir.join(artifact_path);
            }
            let fs = Fs::new();
            fs.create_dir_all(artifact_path.parent().unwrap()).unwrap();
            fs.write(&artifact_path, b"hello world").unwrap();

            let digest_and_type = client
                .add_layer(Layer::Paths {
                    paths: vec![input_path.try_into().unwrap()],
                    prefix_options: prefix_options_factory(artifact_dir),
                })
                .unwrap();
            nonempty![digest_and_type]
        },
        |artifact_dir, layers| {
            let digest = &layers[0].0;
            verify_single_entry_manifest(
                &artifact_dir.join(digest.to_string()),
                &expected_path_factory(&artifact_dir),
                ManifestEntryData::File(Some(hash_data(b"hello world"))),
            )
        },
    );
}

#[test]
fn paths_prefix_strip_and_prepend_absolute() {
    basic_job_with_add_layer_paths_and_prefix(
        |artifact_dir| artifact_dir.join("test_artifact"),
        |artifact_dir| PrefixOptions {
            strip_prefix: Some(artifact_dir.to_owned().try_into().unwrap()),
            prepend_prefix: Some("foo/".into()),
            ..Default::default()
        },
        |_| Path::new("foo/test_artifact").to_owned(),
    )
}

#[test]
fn paths_prefix_strip_and_prepend_relative() {
    basic_job_with_add_layer_paths_and_prefix(
        |_| Path::new("bar/test_artifact").to_owned(),
        |_| PrefixOptions {
            strip_prefix: Some("bar".into()),
            prepend_prefix: Some("foo/".into()),
            ..Default::default()
        },
        |_| Path::new("foo/test_artifact").to_owned(),
    )
}

#[test]
fn paths_prefix_strip_not_found_absolute() {
    basic_job_with_add_layer_paths_and_prefix(
        |artifact_dir| artifact_dir.join("test_artifact"),
        |_| PrefixOptions {
            strip_prefix: Some("not_there/".into()),
            ..Default::default()
        },
        |artifact_dir| artifact_dir.join("test_artifact"),
    )
}

#[test]
fn paths_prefix_strip_not_found_relative() {
    basic_job_with_add_layer_paths_and_prefix(
        |_| Path::new("test_artifact").to_owned(),
        |_| PrefixOptions {
            strip_prefix: Some("not_there/".into()),
            ..Default::default()
        },
        |_| Path::new("test_artifact").to_owned(),
    )
}

#[test]
fn paths_prefix_prepend_absolute() {
    basic_job_with_add_layer_paths_and_prefix(
        |artifact_dir| artifact_dir.join("test_artifact"),
        |_| PrefixOptions {
            prepend_prefix: Some("foo/bar".into()),
            ..Default::default()
        },
        |artifact_dir| {
            Path::new("foo/bar")
                .join(artifact_dir.strip_prefix("/").unwrap())
                .join("test_artifact")
        },
    )
}

#[test]
fn paths_prefix_prepend_relative() {
    basic_job_with_add_layer_paths_and_prefix(
        |_| Path::new("test_artifact").to_owned(),
        |_| PrefixOptions {
            prepend_prefix: Some("foo/bar".into()),
            ..Default::default()
        },
        |_| Path::new("foo/bar/test_artifact").to_owned(),
    )
}

#[test]
fn paths_prefix_canonicalize_relative() {
    basic_job_with_add_layer_paths_and_prefix(
        |_| Path::new("test_artifact").to_owned(),
        |_| PrefixOptions {
            canonicalize: true,
            ..Default::default()
        },
        |artifact_dir| artifact_dir.join("test_artifact"),
    )
}

#[test]
fn paths_prefix_canonicalize_absolute() {
    basic_job_with_add_layer_paths_and_prefix(
        |artifact_dir| artifact_dir.join("test_artifact"),
        |_| PrefixOptions {
            canonicalize: true,
            ..Default::default()
        },
        |artifact_dir| artifact_dir.join("test_artifact"),
    )
}

fn basic_job_with_add_layer_glob_and_prefix(
    glob_factory: impl FnOnce(&Path) -> String,
    input_files: HashMap<&str, &str>,
    prefix_options_factory: impl FnOnce(&Path) -> PrefixOptions,
    expected_path: &Path,
) {
    basic_job_test(
        |client, artifact_dir| {
            let fs = Fs::new();
            for (path, contents) in input_files {
                let artifact = artifact_dir.join(path);
                fs.create_dir_all(artifact.parent().unwrap()).unwrap();
                fs.write(artifact, contents.as_bytes()).unwrap();
            }

            let digest_and_type = client
                .add_layer(Layer::Glob {
                    glob: glob_factory(&artifact_dir),
                    prefix_options: prefix_options_factory(artifact_dir),
                })
                .unwrap();
            nonempty![digest_and_type]
        },
        |artifact_dir, layers| {
            let digest = &layers[0].0;
            verify_single_entry_manifest(
                &artifact_dir.join(digest.to_string()),
                expected_path,
                ManifestEntryData::File(Some(hash_data(b"hello world"))),
            )
        },
    );
}

#[test]
fn glob_basic_relative() {
    basic_job_with_add_layer_glob_and_prefix(
        |_| "*.txt".into(),
        hashmap! {
            "foo.txt" => "hello world",
            "bar.bin" => "hello world",
        },
        |_| PrefixOptions::default(),
        &Path::new("foo.txt"),
    )
}

#[test]
fn glob_strip_and_prepend_prefix_relative() {
    basic_job_with_add_layer_glob_and_prefix(
        |_| "*.txt".into(),
        hashmap! {
            "foo.txt" => "hello world",
            "bar.bin" => "hello world",
        },
        |artifact_dir| PrefixOptions {
            strip_prefix: Some(artifact_dir.to_owned().try_into().unwrap()),
            prepend_prefix: Some("foo/bar".into()),
            ..Default::default()
        },
        &Path::new("foo/bar/foo.txt"),
    )
}

#[test]
fn glob_strip_prefix_relative() {
    basic_job_with_add_layer_glob_and_prefix(
        |_| "*.txt".into(),
        hashmap! {
            "foo.txt" => "hello world",
            "bar.bin" => "hello world",
        },
        |artifact_dir| PrefixOptions {
            strip_prefix: Some(artifact_dir.to_owned().try_into().unwrap()),
            ..Default::default()
        },
        &Path::new("foo.txt"),
    )
}

#[test]
fn glob_prepend_prefix_relative() {
    basic_job_with_add_layer_glob_and_prefix(
        |_| "*.txt".into(),
        hashmap! {
            "foo.txt" => "hello world",
            "bar.bin" => "hello world",
        },
        |_| PrefixOptions {
            prepend_prefix: Some("foo/".into()),
            ..Default::default()
        },
        &Path::new("foo/foo.txt"),
    )
}

#[test]
fn glob_sub_dir_relative() {
    basic_job_with_add_layer_glob_and_prefix(
        |_| "foo/*".into(),
        hashmap! {
            "foo/bar.txt" => "hello world",
            "bar.bin" => "hello world",
        },
        |_| PrefixOptions::default(),
        &Path::new("foo/bar.txt"),
    )
}

#[test]
fn glob_no_files_relative() {
    basic_job_test(
        |client, _artifact_dir| {
            let digest_and_type = client
                .add_layer(Layer::Glob {
                    glob: "*.txt".into(),
                    prefix_options: Default::default(),
                })
                .unwrap();
            nonempty![digest_and_type]
        },
        |artifact_dir, layers| {
            let digest = &layers[0].0;
            verify_empty_manifest(&artifact_dir.join(digest.to_string()))
        },
    );
}

fn stubs_test(path: &str, expected: Vec<(&Utf8Path, ManifestEntryData)>) {
    basic_job_test(
        |client, _| {
            let digest_and_type = client
                .add_layer(Layer::Stubs {
                    stubs: vec![path.to_owned()],
                })
                .unwrap();
            nonempty![digest_and_type]
        },
        |artifact_dir, layers| {
            let digest = &layers[0].0;
            verify_manifest(&artifact_dir.join(digest.to_string()), expected)
        },
    );
}

#[test]
fn stub_file_test() {
    stubs_test(
        "/foo",
        vec![(Utf8Path::new("/foo"), ManifestEntryData::File(None))],
    );
}

#[test]
fn stub_expanded_file_test() {
    stubs_test(
        "/foo/{bar,baz}",
        vec![
            (Utf8Path::new("/foo/bar"), ManifestEntryData::File(None)),
            (Utf8Path::new("/foo/baz"), ManifestEntryData::File(None)),
        ],
    );
}

#[test]
fn stub_dir_test() {
    stubs_test(
        "/foo/",
        vec![(Utf8Path::new("/foo/"), ManifestEntryData::Directory)],
    );
}

#[test]
fn stub_expanded_dir_test() {
    stubs_test(
        "/foo/{bar,baz}/",
        vec![
            (Utf8Path::new("/foo/bar/"), ManifestEntryData::Directory),
            (Utf8Path::new("/foo/baz/"), ManifestEntryData::Directory),
        ],
    );
}

#[test]
fn symlink_test() {
    basic_job_test(
        |client, _| {
            let digest_and_type = client
                .add_layer(Layer::Symlinks {
                    symlinks: vec![SymlinkSpec {
                        link: utf8_path_buf!("/foo"),
                        target: utf8_path_buf!("/bar"),
                    }],
                })
                .unwrap();
            nonempty![digest_and_type]
        },
        |artifact_dir, layers| {
            let digest = &layers[0].0;
            verify_single_entry_manifest(
                &artifact_dir.join(digest.to_string()),
                &Path::new("/foo"),
                ManifestEntryData::Symlink(b"/bar".to_vec()),
            )
        },
    );
}
