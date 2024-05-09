# mypy: disable-error-code="import-untyped"
import grpc
import os
import subprocess

from typing import Optional, Union, Protocol, Sequence
from .items_pb2 import (
    AddLayerRequest,
    ArtifactType,
    GlobLayer,
    ImageSpec,
    JobMount,
    JobSpec,
    LayerSpec,
    PathsLayer,
    RunJobRequest,
    RunJobResponse,
    StartRequest,
    StubsLayer,
    SymlinkSpec,
    SymlinksLayer,
    TarLayer,
)
from .items_pb2_grpc import ClientProcessStub
from xdg_base_dirs import (
    xdg_cache_home,
    xdg_state_home,
)


class RunJobFuture(Protocol):
    def result(self) -> RunJobResponse: ...


LayerType = Union[TarLayer, GlobLayer, PathsLayer, StubsLayer, SymlinksLayer]


class Client:
    def __init__(self, slots: int) -> None:
        client_bin = os.path.join(
            os.path.dirname(os.path.abspath(__file__)), "maelstrom-client"
        )
        proc = subprocess.Popen(
            client_bin, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL
        )
        assert proc.stdout is not None
        address = proc.stdout.readline().strip().decode()

        channel = grpc.insecure_channel(f"unix-abstract:{address}")
        self.stub = ClientProcessStub(channel)

        self.stub.Start(
            StartRequest(
                project_dir=".".encode(),
                state_dir=os.path.join(xdg_state_home(), "maelstrom/py").encode(),
                cache_dir=os.path.join(xdg_cache_home(), "maelstrom/py").encode(),
                cache_size=1024 * 1024 * 1024,
                inline_limit=1024 * 1024,
                slots=slots,
                container_image_depot_dir=os.path.join(
                    xdg_cache_home(), "maelstrom/container"
                ).encode(),
            )
        )

    def add_layer(self, layer: LayerType) -> LayerSpec:
        if isinstance(layer, TarLayer):
            req = AddLayerRequest(tar=layer)
        elif isinstance(layer, GlobLayer):
            req = AddLayerRequest(glob=layer)
        elif isinstance(layer, PathsLayer):
            req = AddLayerRequest(paths=layer)
        elif isinstance(layer, StubsLayer):
            req = AddLayerRequest(stubs=layer)
        elif isinstance(layer, SymlinksLayer):
            req = AddLayerRequest(symlinks=layer)
        else:
            raise RuntimeError(f"unknown layer type {layer!r}")
        return self.stub.AddLayer(req).spec

    def run_job(
        self,
        spec: JobSpec,
    ) -> RunJobFuture:
        return self.stub.RunJob.future(RunJobRequest(spec=spec))
