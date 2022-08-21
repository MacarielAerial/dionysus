from TikTokApi.api.hashtag import Hashtag
from TikTokApi.api.video import Video

from src.dionysus.nodes.api_response_parsing import (
    api_author_to_node_attrs,
    api_hashtag_to_node_attrs,
    api_video_to_node_attrs,
)


def test_api_video_to_node_attrs(  # type: ignore[no-any-unimported]
    mocked_api_video: Video,
) -> None:
    video_node_attrs = api_video_to_node_attrs(video_info=mocked_api_video.info())

    video_node_attrs_native = video_node_attrs.to_dict_native()

    assert video_node_attrs_native["id"] == mocked_api_video.id


def test_api_hashtag_to_node_attrs(  # type: ignore[no-any-unimported]
    mocked_api_hashtag: Hashtag,
) -> None:
    hashtag_node_attrs = api_hashtag_to_node_attrs(
        hashtag_info=mocked_api_hashtag.info()
    )

    hashtag_node_attrs_native = hashtag_node_attrs.to_dict_native()

    assert hashtag_node_attrs_native["id"] == mocked_api_hashtag.id


def test_api_author_to_node_attrs(  # type: ignore[no-any-unimported]
    mocked_api_video_with_author: Video,
) -> None:
    author_node_attrs = api_author_to_node_attrs(
        video_info=mocked_api_video_with_author.info()
    )

    author_node_attrs_native = author_node_attrs.to_dict_native()

    assert (
        author_node_attrs_native["id"]
        == mocked_api_video_with_author.info()["author"]["id"]
    )
