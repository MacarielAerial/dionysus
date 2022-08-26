"""
Parse API response from TikTok into graph elements
"""


from typing import Any, Dict

from .nx_g_schema import NodeAttr, NodeAttrKey, NodeAttrs, NodeType


def video_info_to_video_node_attrs(video_info: Dict[str, Any]) -> NodeAttrs:
    # Identify video statistics
    video_stats = video_info["stats"]

    # Parse each node attribute
    ntype = NodeAttr(
        node_attr_key=NodeAttrKey.ntype, node_attr_val=NodeType.video.value
    )
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
        node_attr_val=video_info["video"].get("definition", None),
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


def hashtag_info_to_hashtag_node_attrs(hashtag_info: Dict[str, Any]) -> NodeAttrs:
    # Parse each node attribute
    ntype = NodeAttr(
        node_attr_key=NodeAttrKey.ntype, node_attr_val=NodeType.hashtag.value
    )
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


def author_info_stats_to_author_node_attrs(
    author_info: Dict[str, Any], author_stats: Dict[str, Any]
) -> NodeAttrs:
    # Parse each node attribute
    ntype = NodeAttr(
        node_attr_key=NodeAttrKey.ntype, node_attr_val=NodeType.author.value
    )
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
    following_count = NodeAttr(
        node_attr_key=NodeAttrKey.following_count,
        node_attr_val=author_stats["followingCount"],
    )
    heart = NodeAttr(
        node_attr_key=NodeAttrKey.heart, node_attr_val=author_stats["heart"]
    )
    digg_count = NodeAttr(
        node_attr_key=NodeAttrKey.digg_count, node_attr_val=author_stats["diggCount"]
    )
    video_count = NodeAttr(
        node_attr_key=NodeAttrKey.video_count, node_attr_val=author_stats["videoCount"]
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
            following_count,
            heart,
            digg_count,
            video_count,
        ]
    )

    return author_node_attrs


def music_info_to_music_node_attrs(music_info: Dict[str, Any]) -> NodeAttrs:
    # Parse each node attribute
    ntype = NodeAttr(
        node_attr_key=NodeAttrKey.ntype, node_attr_val=NodeType.music.value
    )
    id = NodeAttr(node_attr_key=NodeAttrKey.id, node_attr_val=music_info["id"])
    text = NodeAttr(node_attr_key=NodeAttrKey.text, node_attr_val=music_info["title"])
    author_name = NodeAttr(
        node_attr_key=NodeAttrKey.author_name, node_attr_val=music_info["authorName"]
    )
    album = NodeAttr(node_attr_key=NodeAttrKey.album, node_attr_val=music_info["album"])
    duration = NodeAttr(
        node_attr_key=NodeAttrKey.duration, node_attr_val=music_info["duration"]
    )
    play_url = NodeAttr(
        node_attr_key=NodeAttrKey.play_url, node_attr_val=music_info["playUrl"]
    )

    # Collect parsed node attributes
    music_node_attrs = NodeAttrs(
        list_node_attr=[ntype, id, text, author_name, album, duration, play_url]
    )

    return music_node_attrs
