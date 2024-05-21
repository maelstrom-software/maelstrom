#!/usr/bin/env python
import os
import pytest
import shutil
import subprocess
import sys
import threading

from _pytest.nodes import Node as PytestNode
from tqdm import tqdm
from typing import Optional, Sequence, Tuple, List
from contextlib import redirect_stdout
from io import StringIO
from maelstrom_client import (
    AddLayerRequest,
    Client,
    Duration,
    EnvironmentSpec,
    GlobLayer,
    ImageSpec,
    JobDevice,
    JobMount,
    JobNetwork,
    JobSpec,
    LayerType,
    PathsLayer,
    PrefixOptions,
    ProcMount,
    RunJobFuture,
    StubsLayer,
    TmpMount,
)
from maelstrom_test_config import (
    testing_layers,
    testing_devices,
    testing_mounts,
    ENABLE_WRITABLE_FILE_SYSTEM,
)


class Plugin:
    def __init__(self) -> None:
        self.items: List[pytest.Item] = []

    def pytest_collection_modifyitems(
        self, session: pytest.Session, config: pytest.Config, items: List[pytest.Item]
    ) -> None:
        self.items = items


def collect_pytest_tests() -> List[pytest.Item]:
    plugin = Plugin()
    with redirect_stdout(StringIO()):
        pytest.main(args=["--co"], plugins=[plugin])
    return plugin.items


def create_venv(requirements: str, dest: str) -> None:
    os.makedirs(dest, exist_ok=True)

    cached_requirements = os.path.join(dest, "requirements.txt")
    if os.path.exists(cached_requirements):
        with open(requirements, "r") as req_in:
            with open(cached_requirements, "r") as cached_req:
                if req_in.read() == cached_req.read():
                    print("no update to venv needed")
                    return

    subprocess.check_output(["python", "-m", "venv", dest])
    subprocess.check_output(
        [
            "/bin/bash",
            "-c",
            f"source {dest}/bin/activate && pip install --ignore-installed -r {requirements}",
        ],
    )
    shutil.copyfile(requirements, cached_requirements)


def format_duration(dur: Duration) -> str:
    frac = dur.nano_seconds / 100_000_000
    return f"{dur.seconds}.{int(frac)}s"


def wait_for_job(name: str, job: RunJobFuture, failed: List[str], bar: tqdm) -> None:
    result = job.result()
    if result.result.HasField("outcome"):
        if result.result.outcome.completed.exited == 0:
            dur = format_duration(result.result.outcome.completed.effects.duration)
            bar.write(f"{name} completed success took {dur}")
        else:
            bar.write(f"{name} completed failure")
            stdout = result.result.outcome.completed.effects.stdout.inline.decode()
            sys.stdout.write(stdout)
            stderr = result.result.outcome.completed.effects.stderr.inline.decode()
            sys.stderr.write(stderr)
            failed.append(name)
    else:
        print("error:", str(result.result.error).strip())
        failed.append(name)
    bar.update()


def get_python_version() -> str:
    return f"{sys.version_info.major}.{sys.version_info.minor}"


def build_test_name(start: pytest.Item) -> str:
    base_parent = None

    def get_info(item: PytestNode) -> Optional[Tuple[str, str]]:
        if hasattr(item, "reportinfo"):
            (file, _, case) = item.reportinfo()
            return (str(file), case)
        else:
            return None

    computed_file = None
    computed_case = None

    i: PytestNode = start
    while True:
        i_info = get_info(i)
        if i_info is not None:
            (file, case_) = i_info
            computed_file = file

            if computed_case is None:
                computed_case = case_.replace(".", "::")
            else:
                computed_case = case_.replace(".", "::") + "::" + computed_case

            if not file.endswith(".py"):
                computed_case = os.path.basename(file) + "::" + computed_case

            if file.endswith(".py"):
                break
        assert i.parent is not None
        i = i.parent
    assert computed_file is not None
    assert computed_case is not None

    base_parent = i

    computed_file = os.path.relpath(computed_file, ".")
    return f"{computed_file}::{computed_case}"


def main() -> None:
    test_filter = sys.argv[1] if len(sys.argv) > 1 else None
    client = Client(slots=24)

    python_version = get_python_version()
    image = ImageSpec(
        name="python",
        tag=f"{python_version}-slim",
        use_layers=True,
        use_environment=True,
    )

    work = os.path.abspath(".")
    venv_dir = "target/maelstrom_venv"

    print("creating venv")
    create_venv("test-requirements.txt", venv_dir)

    print("creating layers")
    layers = []
    layers.append(
        client.add_layer(
            GlobLayer(
                glob=f"{venv_dir}/lib/python{python_version}/site-packages/**",
                prefix_options=PrefixOptions(
                    canonicalize=False,
                    follow_symlinks=False,
                    strip_prefix=f"{venv_dir}/",
                    prepend_prefix=f"/usr/local/",
                ),
            )
        )
    )

    for layer in testing_layers(work):
        layers.append(client.add_layer(layer))

    print("collecting tests")
    tests = collect_pytest_tests()

    bar = tqdm(total=len(tests))
    bar.write(f"enqueuing {len(tests)} tests")
    job_threads = []
    failed: List[str] = []
    for item in tests:
        test_name = build_test_name(item)

        if test_filter and test_filter not in test_name:
            continue

        script = f"/usr/local/bin/python -m pytest --verbose {test_name}"

        spec = JobSpec(
            program="/bin/sh",
            arguments=["-c", script],
            image=image,
            layers=layers,
            user=0,
            group=0,
            devices=testing_devices(),
            mounts=testing_mounts(work),
            network=JobNetwork.Loopback,
            working_directory=work,
            enable_writable_file_system=ENABLE_WRITABLE_FILE_SYSTEM,
        )
        job = client.run_job(spec)
        t = threading.Thread(target=wait_for_job, args=(test_name, job, failed, bar))
        t.start()
        job_threads.append(t)

    bar.write(f"done enqueuing, waiting for {len(job_threads)} jobs")

    for t in job_threads:
        t.join()

    if failed:
        print(f"{len(failed)} tests failed")
        for f in failed:
            print(f"    {f}")


if __name__ == "__main__":
    main()
