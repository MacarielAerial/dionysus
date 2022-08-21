from TikTokApi.api.hashtag import Hashtag
from TikTokApi.api.video import Video

from src.dionysus.nodes.api_response_parsing import (
    author_info_stats_to_author_node_attrs,
    hashtag_info_to_hashtag_node_attrs,
    music_info_to_music_node_attrs,
    video_info_to_video_node_attrs,
)


def test_api_video_to_node_attrs(  # type: ignore[no-any-unimported]
    mocked_api_video: Video,
) -> None:
    video_node_attrs = video_info_to_video_node_attrs(
        video_info=mocked_api_video.info()
    )

    video_node_attrs_native = video_node_attrs.to_dict_native()

    assert video_node_attrs_native["id"] == mocked_api_video.id


def test_hashtag_info_to_node_attrs(  # type: ignore[no-any-unimported]
    mocked_api_hashtag: Hashtag,
) -> None:
    hashtag_node_attrs = hashtag_info_to_hashtag_node_attrs(
        hashtag_info=mocked_api_hashtag.info()
    )

    hashtag_node_attrs_native = hashtag_node_attrs.to_dict_native()

    assert hashtag_node_attrs_native["id"] == mocked_api_hashtag.id


def test_author_info_stats_to_author_node_attrs(  # type: ignore[no-any-unimported]
    mocked_api_video_with_author: Video,
) -> None:
    author_node_attrs = author_info_stats_to_author_node_attrs(
        author_info=mocked_api_video_with_author.info()["author"],
        author_stats=mocked_api_video_with_author.info()["authorStats"],
    )

    author_node_attrs_native = author_node_attrs.to_dict_native()

    assert (
        author_node_attrs_native["id"]
        == mocked_api_video_with_author.info()["author"]["id"]
    )


def test_api_music_to_music_node_attrs(  # type: ignore[no-any-unimported]
    mocked_api_video_with_music: Video,
) -> None:
    music_node_attrs = music_info_to_music_node_attrs(
        music_info=mocked_api_video_with_music.info()["music"]
    )

    music_node_attrs_native = music_node_attrs.to_dict_native()

    assert (
        music_node_attrs_native["id"]
        == mocked_api_video_with_music.info()["music"]["id"]
    )
