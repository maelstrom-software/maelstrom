import subprocess

from typing import List
from maelstrom_client import (
    GlobLayer,
    JobDevice,
    JobMount,
    LayerType,
    PathsLayer,
    PrefixOptions,
    ProcMount,
    StubsLayer,
    TmpMount,
)

ENABLE_WRITABLE_FILE_SYSTEM = False

def get_shared_library_deps(path: str) -> List[str]:
    paths = []
    output = subprocess.check_output(["ldd", path]).decode()
    for line in output.splitlines():
        if "ld-linux" in line:
            paths.append(line.split(" => ")[0].strip())
        elif " => " in line:
            paths.append(line.split(" => ")[1].split(" ")[0])
    return paths

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
