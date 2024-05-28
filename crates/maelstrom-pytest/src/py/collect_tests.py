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
    tests = collect_pytest_tests()
    for item in tests:
        test_name = build_test_name(item)
        print(test_name)


if __name__ == "__main__":
    main()
