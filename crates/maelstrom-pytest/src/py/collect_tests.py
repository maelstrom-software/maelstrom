#!/usr/bin/env python
import os
import pytest

from _pytest.nodes import Node as PytestNode
from typing import Optional, Tuple, List
from contextlib import redirect_stdout
from io import StringIO


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


def main() -> None:
    tests = collect_pytest_tests()
    for item in tests:
        print(item.nodeid)


if __name__ == "__main__":
    main()
