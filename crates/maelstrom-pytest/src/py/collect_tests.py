#!/usr/bin/env python
import os
import pytest
import json

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


def collect_pytest_tests() -> List[pytest.Item]:
    plugin = Plugin()
    with redirect_stdout(StringIO()):
        pytest.main(args=["--co"], plugins=[plugin])
    return plugin.items

@dataclass(frozen=True)
class TestCase:
    file: str
    name: str
    node_id: str
    markers: Sequence[str]

def main() -> None:
    tests = collect_pytest_tests()
    for item in tests:
        (raw_file, _, name) = item.reportinfo()
        file = str(raw_file)
        markers = [m.name for m in item.own_markers]

        case = TestCase(file=file, name=name, node_id=item.nodeid, markers=markers)
        print(json.dumps(asdict(case)))


if __name__ == "__main__":
    main()
