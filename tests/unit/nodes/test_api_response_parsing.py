from TikTokApi.api.video import Video

from src.dionysus.nodes.api_response_parsing import api_video_to_node_attrs


def test_api_video_to_node_attrs(  # type: ignore[no-any-unimported]
    mocked_api_video_generator: Video,
) -> None:
    node_attrs = api_video_to_node_attrs(api_video=mocked_api_video_generator)

    assert node_attrs.list_node_attr[1].node_attr_val == mocked_api_video_generator.id
