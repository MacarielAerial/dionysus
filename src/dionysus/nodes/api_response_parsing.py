"""
Parse API response from TikTok into graph elements
"""


from typing import Any, Dict

from .nx_g_schema import NodeAttr, NodeAttrKey, NodeAttrs, NodeType


def api_video_to_node_attrs(video_info: Dict[str, Any]) -> NodeAttrs:
    # Identify video statistics
    video_stats = video_info["stats"]

    # Parse each node attribute
    ntype = NodeAttr(node_attr_key=NodeAttrKey.ntype, node_attr_val=NodeType.video)
    id = NodeAttr(node_attr_key=NodeAttrKey.id, node_attr_val=video_info["id"])
    text = NodeAttr(node_attr_key=NodeAttrKey.text, node_attr_val=video_info["desc"])
    creation_time = NodeAttr(
        node_attr_key=NodeAttrKey.creation_time, node_attr_val=video_info["createTime"]
    )
    duration = NodeAttr(
        node_attr_key=NodeAttrKey.duration,
        node_attr_val=video_info["video"]["duration"],
    )
    width = NodeAttr(
        node_attr_key=NodeAttrKey.width, node_attr_val=video_info["video"]["width"]
    )
    height = NodeAttr(
        node_attr_key=NodeAttrKey.height, node_attr_val=video_info["video"]["height"]
    )
    definition = NodeAttr(
        node_attr_key=NodeAttrKey.definition,
        node_attr_val=video_info["video"]["definition"],
    )
    format = NodeAttr(
        node_attr_key=NodeAttrKey.format, node_attr_val=video_info["video"]["format"]
    )
    comment_count = NodeAttr(
        node_attr_key=NodeAttrKey.comment_count,
        node_attr_val=video_stats["commentCount"],
    )
    play_count = NodeAttr(
        node_attr_key=NodeAttrKey.play_count, node_attr_val=video_stats["playCount"]
    )
    share_count = NodeAttr(
        node_attr_key=NodeAttrKey.share_count, node_attr_val=video_stats["shareCount"]
    )
    digg_count = NodeAttr(
        node_attr_key=NodeAttrKey.digg_count, node_attr_val=video_stats["diggCount"]
    )

    # Collect parsed node attributes
    video_attrs = NodeAttrs(
        list_node_attr=[
            ntype,
            id,
            text,
            creation_time,
            duration,
            width,
            height,
            definition,
            format,
            comment_count,
            play_count,
            share_count,
            digg_count,
        ]
    )

    return video_attrs


def api_hashtag_to_node_attrs(hashtag_info: Dict[str, Any]) -> NodeAttrs:
    # Parse each node attributes
    ntype = NodeAttr(node_attr_key=NodeAttrKey.ntype, node_attr_val=NodeType.hashtag)
    id = NodeAttr(node_attr_key=NodeAttrKey.id, node_attr_val=hashtag_info["id"])
    text = NodeAttr(node_attr_key=NodeAttrKey.text, node_attr_val=hashtag_info["title"])
    view_count = NodeAttr(
        node_attr_key=NodeAttrKey.view_count,
        node_attr_val=hashtag_info["stats"]["viewCount"],
    )
    video_count = NodeAttr(
        node_attr_key=NodeAttrKey.video_count,
        node_attr_val=hashtag_info["stats"]["videoCount"],
    )

    # Collect parsed attributes
    hashtag_attrs = NodeAttrs(list_node_attr=[ntype, id, text, view_count, video_count])

    return hashtag_attrs


def api_author_to_node_attrs(video_info: Dict[str, Any]) -> NodeAttrs:
    # Identify the section of video info for its author
    author_info = video_info["author"]
    author_stats = video_info["authorStats"]

    # Parse each node attributes
    ntype = NodeAttr(node_attr_key=NodeAttrKey.ntype, node_attr_val=NodeType.author)
    id = NodeAttr(node_attr_key=NodeAttrKey.id, node_attr_val=author_info["id"])
    text = NodeAttr(
        node_attr_key=NodeAttrKey.text, node_attr_val=author_info["uniqueId"]
    )
    nickname = NodeAttr(
        node_attr_key=NodeAttrKey.nickname, node_attr_val=author_info["nickname"]
    )
    is_private_account = NodeAttr(
        node_attr_key=NodeAttrKey.is_private_account,
        node_attr_val=author_info["privateAccount"],
    )
    signature = NodeAttr(
        node_attr_key=NodeAttrKey.signature, node_attr_val=author_info["signature"]
    )
    verified = NodeAttr(
        node_attr_key=NodeAttrKey.verified, node_attr_val=author_info["verified"]
    )
    follower_count = NodeAttr(
        node_attr_key=NodeAttrKey.follower_count,
        node_attr_val=author_stats["followerCount"],
    )
    heart = NodeAttr(
        node_attr_key=NodeAttrKey.heart, node_attr_val=author_stats["heart"]
    )
    digg_count = NodeAttr(
        node_attr_key=NodeAttrKey.digg_count, node_attr_val=author_stats["diggCount"]
    )

    # Collect parsed attributes
    author_node_attrs = NodeAttrs(
        list_node_attr=[
            ntype,
            id,
            text,
            nickname,
            is_private_account,
            signature,
            verified,
            follower_count,
            heart,
            digg_count,
        ]
    )

    return author_node_attrs
