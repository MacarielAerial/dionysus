from __future__ import annotations

from logging import Logger
from pathlib import Path

from pytest import fixture

from src.dionysus.nodes.base_logger import get_base_logger


@fixture
def test_logger() -> Logger:
    return get_base_logger()


@fixture
def test_fixture() -> TestFixture:
    return TestFixture()


class TestFixture:
    @property
    def path_own_file(self) -> Path:
        return Path(__file__)

    @property
    def path_dir_test(self) -> Path:
        return self.path_own_file.parent
