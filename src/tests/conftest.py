from __future__ import annotations

import logging
from logging import Logger
from pathlib import Path

from pytest import fixture

from dionysus.nodes.project_logging import default_logging


@fixture
def test_logger() -> Logger:
    default_logging()

    logger = logging.getLogger(__name__)

    return logger


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
