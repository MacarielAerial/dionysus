from __future__ import annotations

from logging import Logger
from pathlib import Path
from unittest.mock import Mock, patch

from pytest import fixture
from TikTokApi.api.hashtag import Hashtag
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
def mocked_api_video() -> Video:  # type: ignore[no-any-unimported]
    with patch("TikTokApi.api.video.Video"):
        video = Video(id="7057847459820604677")
        video.info_full = Mock()
        video.info_full.return_value = {
            "itemInfo": {
                "itemStruct": {
                    "id": "7057847459820604677",
                    "desc": "#zoukbrasileiro",
                    "createTime": 1643283168,
                    "video": {
                        "definition": "720p",
                        "duration": 28,
                        "format": "mp4",
                        "height": 1024,
                        "width": 576,
                    },
                    "stats": {
                        "commentCount": 2,
                        "diggCount": 499,
                        "playCount": 8646,
                        "shareCount": 6,
                    },
                }
            }
        }

        yield video


@fixture
def mocked_api_hashtag() -> Hashtag:  # type: ignore[no-any-unimported]
    with patch("TikTokApi.api.hashtag.Hashtag"):
        hashtag = Hashtag(id="74915315")
        hashtag.info_full = Mock()
        hashtag.info_full.return_value = {
            "challengeInfo": {
                "challenge": {
                    "id": "74915315",
                    "stats": {"videoCount": 1405, "viewCount": 2700000},
                    "title": "zoukbrasileiro",
                }
            }
        }

        yield hashtag


@fixture
def mocked_api_video_with_author() -> Video:  # type: ignore[no-any-unimported]
    with patch("TikTokApi.api.video.Video"):
        video = Video(id="7057847459820604677")
        video.info_full = Mock()
        video.info_full.return_value = {
            "itemInfo": {
                "itemStruct": {
                    "author": {
                        "id": "6813185604563567621",
                        "nickname": "Fabricio B Durães",
                        "privateAccount": False,
                        "signature": "Te ensino o passo para a felicidade \n"
                        "Especialista em Zouk \n"
                        "Professor \n"
                        "Produtor",
                        "uniqueId": "fabricioduraes0",
                        "verified": False,
                    },
                    "authorStats": {
                        "diggCount": 193,
                        "followerCount": 61100,
                        "followingCount": 100,
                        "heart": 351300,
                        "heartCount": 351300,
                        "videoCount": 127,
                    },
                }
            }
        }

        yield video


@fixture
def mocked_api_video_with_music() -> Video:  # type: ignore[no-any-unimported]
    with patch("TikTokApi.api.video.Video"):
        video = Video(id="7057847459820604677")
        video.info_full = Mock()
        video.info_full.return_value = {
            "itemInfo": {
                "itemStruct": {
                    "music": {
                        "album": "Faking Love (feat. Saweetie)",
                        "authorName": "Anitta",
                        "duration": 60,
                        "id": "7020499784389625857",
                        "playUrl": "https://sf16-ies-music-va.tiktokcdn.com/obj/"
                        "tos-useast2a-ve-2774/6f456cc79d844251aa7884bd45938e0a",
                        "title": "Faking Love (feat. Saweetie)",
                    },
                }
            }
        }

        yield video
