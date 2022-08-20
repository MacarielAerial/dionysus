"""
Parse API response from TikTok into graph elements
"""


from TikTokApi.api.video import Video

from .nx_g_schema import NodeAttr, NodeAttrKey, NodeAttrs, NodeType


def api_video_to_node_attrs(  # type: ignore[no-any-unimported]
    api_video: Video,
) -> NodeAttrs:
    # Assign space for intermediate object
    video_info = api_video.info()

    # Parse each node attribute
    ntype = NodeAttr(node_attr_key=NodeAttrKey.ntype, node_attr_val=NodeType.video)
    id = NodeAttr(node_attr_key=NodeAttrKey.id, node_attr_val=api_video.id)
    text = NodeAttr(node_attr_key=NodeAttrKey.text, node_attr_val=video_info["desc"])
    creation_time = NodeAttr(
        node_attr_key=NodeAttrKey.creation_time, node_attr_val=video_info["createTime"]
    )

    # Collect parsed node attributes
    video_attrs = NodeAttrs(list_node_attr=[ntype, id, text, creation_time])

    return video_attrs
