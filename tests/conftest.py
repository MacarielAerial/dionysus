from __future__ import annotations

from logging import Logger
from pathlib import Path
from unittest.mock import Mock, patch

from pytest import fixture
from TikTokApi.api.video import Video

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


@fixture
def mocked_api_video_generator() -> Video:  # type: ignore[no-any-unimported]
    with patch("TikTokApi.api.video.Video"):
        video = Video(id="7057847459820604677")
        video.info_full = Mock()
        video.info_full.return_value = {
            "itemInfo": {
                "itemStruct": {"desc": "#zoukbrasileiro", "createTime": 1643283168}
            }
        }

        yield video
