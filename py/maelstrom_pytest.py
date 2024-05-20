#!/usr/bin/env python
import os
import pytest
import subprocess
import sys
import threading

from typing import Sequence, List
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
    if not os.path.exists(dest):
        os.mkdir(dest)
    subprocess.check_output(["python", "-m", "venv", dest])
    subprocess.check_output(
        [
            "/bin/bash",
            "-c",
            f"source {dest}/bin/activate && pip install --ignore-installed -r {requirements}",
        ],
    )


def format_duration(dur: Duration) -> str:
    frac = dur.nano_seconds / 100_000_000
    return f"{dur.seconds}.{int(frac)}s"


def get_shared_library_deps(path: str) -> List[str]:
    paths = []
    output = subprocess.check_output(["ldd", path]).decode()
    for line in output.splitlines():
        if "ld-linux" in line:
            paths.append(line.split(" => ")[0].strip())
        elif " => " in line:
            paths.append(line.split(" => ")[1].split(" ")[0])
    return paths


def wait_for_job(name: str, job: RunJobFuture) -> None:
    result = job.result()
    if result.result.HasField("outcome"):
        if result.result.outcome.completed.exited == 0:
            dur = format_duration(result.result.outcome.completed.effects.duration)
            print(f"{name} completed success took {dur}")
        else:
            print(f"{name} completed failure")
            stdout = result.result.outcome.completed.effects.stdout.inline.decode()
            sys.stdout.write(stdout)
            stderr = result.result.outcome.completed.effects.stderr.inline.decode()
            sys.stderr.write(stderr)
    else:
        print("error:", str(result.result.error).strip())


def testing_layers(work: str) -> List[LayerType]:
    return [
        StubsLayer(
            stubs=[
                "/dev/{null,random,urandom,fuse}",
                "/{proc,tmp,root}/",
                f"{work}/.pytest_cache/",
            ]
        ),
        GlobLayer(
            glob="py/**.{py,pyc}",
            prefix_options=PrefixOptions(
                canonicalize=False, follow_symlinks=False, prepend_prefix=work
            ),
        ),
        GlobLayer(
            glob="target/py/**.{py,pyc}",
            prefix_options=PrefixOptions(
                canonicalize=False, follow_symlinks=False, prepend_prefix=work
            ),
        ),
        PathsLayer(
            paths=[
                "py/maelstrom_client/maelstrom-client",
                "crates/maelstrom-worker/src/executor-test-deps.tar",
            ],
            prefix_options=PrefixOptions(
                canonicalize=False, follow_symlinks=True, prepend_prefix=work
            ),
        ),
        PathsLayer(
            paths=get_shared_library_deps("py/maelstrom_client/maelstrom-client"),
            prefix_options=PrefixOptions(canonicalize=False, follow_symlinks=True),
        ),
    ]


def testing_devices() -> List[JobDevice]:
    return [
        JobDevice.Null,
        JobDevice.Random,
        JobDevice.Urandom,
        JobDevice.Fuse,
    ]


def testing_mounts(work: str) -> List[JobMount]:
    return [
        JobMount(tmp=TmpMount(mount_point="/tmp")),
        JobMount(proc=ProcMount(mount_point="/proc")),
        JobMount(tmp=TmpMount(mount_point=f"/{work}/.pytest_cache")),
        JobMount(tmp=TmpMount(mount_point=f"/root")),
    ]


def main() -> None:
    client = Client(slots=24)
    image = ImageSpec(
        name="python", tag="3.11.9-slim", use_layers=True, use_environment=True
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
                glob=f"{venv_dir}/lib/python3.11/site-packages/**",
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

    print("enqueuing")
    job_threads = []
    for item in tests:
        (file, _, case_) = item.reportinfo()
        if not str(file).endswith(".py"):
            continue
        file = os.path.relpath(file, ".")

        case_ = case_.replace(".", "::")
        script = f"/usr/local/bin/python -m pytest --verbose {file}::{case_}"

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
        )
        job = client.run_job(spec)
        t = threading.Thread(target=wait_for_job, args=(f"{file}::{case_}", job))
        t.start()
        job_threads.append(t)
    print(f"running {len(job_threads)} jobs")

    for t in job_threads:
        t.join()


if __name__ == "__main__":
    main()
