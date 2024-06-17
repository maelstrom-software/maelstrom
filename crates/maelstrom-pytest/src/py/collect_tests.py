#!/usr/bin/env python
import json
import os
import pytest
import sys

from _pytest.nodes import Node as PytestNode
from contextlib import redirect_stdout
from dataclasses import asdict, dataclass
from io import StringIO
from typing import Optional, Sequence, Tuple, List


class Plugin:
    def __init__(self) -> None:
        self.items: List[pytest.Item] = []

    def pytest_collection_modifyitems(
        self, session: pytest.Session, config: pytest.Config, items: List[pytest.Item]
    ) -> None:
        self.items = items


def collect_pytest_tests(args: Sequence[str]) -> List[pytest.Item]:
    plugin = Plugin()
    output = StringIO()
    with redirect_stdout(output):
        ret = pytest.main(args=["--co"] + args, plugins=[plugin])
    if ret != 0:
        output.seek(0)
        sys.stderr.write(output.read())
        os._exit(ret)

    return plugin.items


@dataclass(frozen=True)
class TestCase:
    file: str
    name: str
    node_id: str
    markers: Sequence[str]
    skip: bool


def is_skip(marker: pytest.Mark) -> bool:
    if marker.name == "skip":
        return True
    elif marker.name == "skipif":
        return marker.args[0]
    else:
        return False


def main() -> None:
    args = sys.argv[1:]

    tests = collect_pytest_tests(args)
    for item in tests:
        (raw_file, _, name) = item.reportinfo()
        file = str(raw_file)
        markers = [m.name for m in item.own_markers]

        skip = any(is_skip(m) for m in item.own_markers)

        case = TestCase(
            file=file, name=name, node_id=item.nodeid, markers=markers, skip=skip
        )
        print(json.dumps(asdict(case)))


if __name__ == "__main__":
    main()
