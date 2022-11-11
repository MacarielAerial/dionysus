from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from enum import Enum
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import networkx as nx
from networkx import DiGraph

logger = logging.getLogger(__name__)


class NodeAttrKey(Enum):
    # Hashtag
    ntype: str = "ntype"
    id: str = "id"
    title: str = "title"
    view_count: str = "view_count"
    video_count: str = "video_count"
    timestamp: str = "timestamp"

    # Union with Video
    desc: str = "desc"
    create_time: str = "create_time"
    duration: str = "duration"
    width: str = "width"
    height: str = "height"
    defintion: str = "definition"
    format: str = "format"
    comment_count: str = "comment_count"
    play_count: str = "play_count"
    share_count: str = "share_count"
    digg_count: str = "digg_count"

    # Union with Author
    unique_id: str = "unique_id"
    nickname: str = "nickname"
    is_private_account: str = "is_private_account"
    signature: str = "signature"
    verified: str = "verified"
    follower_count: str = "follower_count"
    following_count: str = "following_count"
    heart: str = "heart"

    # Union with Music
    author_name: str = "author_name"
    album: str = "album"
    play_url: str = "play_url"


class NodeType(Enum):
    hashtag: str = "Hashtag"
    video: str = "Video"
    author: str = "Author"
    music: str = "Music"


class EdgeAttrKey(Enum):
    etype: str = "etype"


class EdgeType(Enum):
    video_to_hashtag: str = "VideoToHashtag"
    author_to_video: str = "AuthorToVideo"
    video_to_music: str = "VideoToMusic"


@dataclass
class Hashtag:
    ntype: NodeType
    id: int
    title: str
    view_count: int
    video_count: int
    timestamp: int

    @classmethod
    def from_dict(cls, dict_response: Dict[str, Any]) -> Hashtag:
        # assert 'extra' in dict_response.keys()
        extra = dict_response["extra"]
        challenge_info = dict_response["challengeInfo"]
        challenge = challenge_info["challenge"]
        stats = challenge["stats"]

        # Parse each node attribute
        ntype = NodeType.hashtag
        id = int(challenge["id"])
        title = str(challenge["title"])
        view_count = int(stats["viewCount"])
        video_count = int(stats["videoCount"])
        timestamp = int(extra["now"])

        # Assemble the dataclass instance
        hashtag = cls(
            ntype=ntype,
            id=id,
            title=title,
            view_count=view_count,
            video_count=video_count,
            timestamp=timestamp,
        )

        return hashtag

    def to_node_attrs(self) -> Dict[str, Any]:
        return {
            NodeAttrKey.ntype.value: self.ntype.value,
            NodeAttrKey.id.value: self.id,
            NodeAttrKey.title.value: self.title,
            NodeAttrKey.view_count.value: self.view_count,
            NodeAttrKey.video_count.value: self.video_count,
            NodeAttrKey.timestamp.value: self.timestamp,
        }


@dataclass
class Video:
    ntype: NodeType
    id: int
    desc: str
    create_time: int
    duration: str
    width: int
    height: int
    definition: Optional[str]
    format: Optional[str]
    comment_count: int
    play_count: int
    share_count: int
    digg_count: int
    time_stamp: int

    @classmethod
    def from_dict(cls, dict_info: Dict[str, Any]) -> Video:
        # Parse each node attribute
        ntype = NodeType.video
        id = int(dict_info["id"])
        desc = str(dict_info["desc"])
        create_time = int(dict_info["createTime"])
        duration = str(dict_info["video"]["duration"])
        width = int(dict_info["video"]["width"])
        height = int(dict_info["video"]["height"])
        definition = str(dict_info["video"].get("definition", None))
        format = str(dict_info["video"].get("format", None))
        comment_count = int(dict_info["stats"]["commentCount"])
        play_count = int(dict_info["stats"]["playCount"])
        share_count = int(dict_info["stats"]["shareCount"])
        digg_count = int(dict_info["stats"]["diggCount"])
        time_stamp = int(dict_info["now"])

        # Assemble the dataclass instance
        video = cls(
            ntype=ntype,
            id=id,
            desc=desc,
            create_time=create_time,
            duration=duration,
            width=width,
            height=height,
            definition=definition,
            format=format,
            comment_count=comment_count,
            play_count=play_count,
            share_count=share_count,
            digg_count=digg_count,
            time_stamp=time_stamp,
        )

        return video

    def to_node_attrs(self) -> Dict[str, Any]:
        return {
            NodeAttrKey.ntype.value: self.ntype.value,
            NodeAttrKey.id.value: self.id,
            NodeAttrKey.desc.value: self.desc,
            NodeAttrKey.create_time.value: self.create_time,
            NodeAttrKey.duration.value: self.duration,
            NodeAttrKey.width.value: self.width,
            NodeAttrKey.height.value: self.height,
            NodeAttrKey.defintion.value: self.definition,
            NodeAttrKey.format.value: self.format,
            NodeAttrKey.comment_count.value: self.comment_count,
            NodeAttrKey.play_count.value: self.play_count,
            NodeAttrKey.share_count.value: self.share_count,
            NodeAttrKey.digg_count.value: self.digg_count,
            NodeAttrKey.timestamp.value: self.time_stamp,
        }


@dataclass
class Author:
    ntype: NodeType
    id: int
    unique_id: str
    nickname: str
    is_private_account: bool
    signature: str
    verified: bool
    follower_count: int
    following_count: int
    heart: int
    digg_count: int
    video_count: int

    @classmethod
    def from_dict(cls, dict_info: Dict[str, Any]) -> Author:
        # Parse each node attribute
        ntype = NodeType.author
        id = int(dict_info["id"])
        unique_id = str(dict_info["uniqueId"])
        nickname = str(dict_info["nickname"])
        is_private_account = bool(dict_info["privateAccount"])
        signature = str(dict_info["signature"])
        verified = bool(dict_info["verified"])
        follower_count = int(dict_info["stats"]["followerCount"])
        following_count = int(dict_info["stats"]["followingCount"])
        heart = int(dict_info["stats"]["heart"])
        digg_count = int(dict_info["stats"]["diggCount"])
        video_count = int(dict_info["stats"]["videoCount"])

        # Assemble the dataclass instance
        author = cls(
            ntype=ntype,
            id=id,
            unique_id=unique_id,
            nickname=nickname,
            is_private_account=is_private_account,
            signature=signature,
            verified=verified,
            follower_count=follower_count,
            following_count=following_count,
            heart=heart,
            digg_count=digg_count,
            video_count=video_count,
        )

        return author

    def to_node_attrs(self) -> Dict[str, Any]:
        return {
            NodeAttrKey.ntype.value: self.ntype.value,
            NodeAttrKey.id.value: self.id,
            NodeAttrKey.unique_id.value: self.unique_id,
            NodeAttrKey.nickname.value: self.nickname,
            NodeAttrKey.is_private_account.value: self.is_private_account,
            NodeAttrKey.signature.value: self.signature,
            NodeAttrKey.verified.value: self.verified,
            NodeAttrKey.follower_count.value: self.follower_count,
            NodeAttrKey.following_count.value: self.following_count,
            NodeAttrKey.heart.value: self.heart,
            NodeAttrKey.digg_count.value: self.digg_count,
            NodeAttrKey.video_count.value: self.video_count,
        }


@dataclass
class Music:
    ntype: NodeType
    id: int
    title: str
    author_name: str
    album: str
    duration: int
    play_url: str

    @classmethod
    def from_dict(cls, dict_info: Dict[str, Any]) -> Music:
        # Parse each node attribute
        ntype = NodeType.music
        id = int(dict_info["id"])
        title = str(dict_info["title"])
        author_name = str(dict_info["authorName"])
        album = str(dict_info["album"])
        duration = int(dict_info["duration"])
        play_url = str(dict_info["playUrl"])

        # Assemble the dataclass instance
        music = cls(
            ntype=ntype,
            id=id,
            title=title,
            author_name=author_name,
            album=album,
            duration=duration,
            play_url=play_url,
        )

        return music

    def to_node_attrs(self) -> Dict[str, Any]:
        return {
            NodeAttrKey.ntype.value: self.ntype.value,
            NodeAttrKey.id.value: self.id,
            NodeAttrKey.title.value: self.title,
            NodeAttrKey.author_name.value: self.author_name,
            NodeAttrKey.album.value: self.album,
            NodeAttrKey.duration.value: self.duration,
            NodeAttrKey.play_url.value: self.play_url,
        }


@dataclass
class VideoSubGraph:  # type: ignore[no-any-unimported]
    nx_g: DiGraph  # type: ignore[no-any-unimported]

    @classmethod
    def from_dict(cls, dict_info: Dict[str, Any]) -> VideoSubGraph:
        # Parse the Video instance
        video = Video.from_dict(dict_info=dict_info)

        # Parse the Hashtag instances
        list_hashtag: List[Hashtag] = []
        for challenge_info in dict_info["challenges"]:
            # Hashtag info from within a video data dictionary requires injection
            # of the below keys and values
            challenge_data: Dict[str, Any] = {
                "challengeInfo": {"challenge": challenge_info},
                "extra": {"now": video.time_stamp},
            }
            hashtag = Hashtag.from_dict(dict_response=challenge_data)
            list_hashtag.append(hashtag)

        # Parse the author instance
        author_info = dict_info["author"]
        author_info["stats"] = dict_info["authorStats"]

        author = Author.from_dict(dict_info=author_info)

        # Parse the music instance
        music = Music.from_dict(dict_info=dict_info["music"])

        # Collate node attributes of nodes of all types
        list_node_attrs: List[Dict[str, Any]] = [
            video.to_node_attrs(),
            author.to_node_attrs(),
            music.to_node_attrs(),
            *[hashtag.to_node_attrs() for hashtag in list_hashtag],
        ]

        # Inject node ids into node attributes to become node tuples
        list_node_tuple: List[Tuple[int, Dict[str, Any]]] = list(
            zip(range(len(list_node_attrs)), list_node_attrs)
        )

        # Derive edge tuples from node tuples
        author_to_video_tuple: Tuple[int, int, Dict[str, Any]] = (
            list_node_tuple[1][0],
            list_node_tuple[0][0],
            {EdgeAttrKey.etype.value: EdgeType.author_to_video.value},
        )
        assert len(author_to_video_tuple) == 3
        video_to_music_tuple: Tuple[int, int, Dict[str, Any]] = (
            list_node_tuple[0][0],
            list_node_tuple[2][0],
            {EdgeAttrKey.etype.value: EdgeType.video_to_music.value},
        )
        list_video_to_hashtag_tuple: List[Tuple[int, int, Dict[str, Any]]] = []
        for i in range(len(list_node_tuple[3:])):
            video_to_hashtag_tuple: Tuple[int, int, Dict[str, Any]] = (
                list_node_tuple[0][0],
                list_node_tuple[i][0],
                {EdgeAttrKey.etype.value: EdgeType.video_to_hashtag.value},
            )
            list_video_to_hashtag_tuple.append(video_to_hashtag_tuple)

        # Collate edge tuples of all types together
        list_edge_tuple: List[Tuple[int, int, Dict[str, Any]]] = [
            author_to_video_tuple,
            video_to_music_tuple,
            *list_video_to_hashtag_tuple,
        ]

        # Initiate a networkx graph from these elements
        nx_g = nx.DiGraph()
        nx_g.add_nodes_from(list_node_tuple)
        nx_g.add_edges_from(list_edge_tuple)

        logger.debug(
            f"TikTok video subgraph contains {nx_g.number_of_nodes()} nodes "
            f"and {nx_g.number_of_edges()} edges"
        )

        return cls(nx_g=nx_g)


class TikTokGraphDataInterface:
    def __init__(self, filepath: Path) -> None:
        self.filepath = filepath

    def save(self, data: DiGraph) -> None:  # type: ignore[no-any-unimported]
        self._save(filepath=self.filepath, tiktok_graph=data)

    @staticmethod
    def _save(  # type: ignore[no-any-unimported]
        filepath: Path, tiktok_graph: DiGraph
    ) -> None:
        with open(filepath, "w") as f:
            serialised = nx.node_link_data(G=tiktok_graph)
            json.dump(serialised, f)

            logger.info(f"Saved a {type(tiktok_graph)} object to {filepath}")

    def load(self) -> DiGraph:  # type: ignore[no-any-unimported]
        return self._load(filepath=self.filepath)

    @staticmethod
    def _load(filepath: Path) -> DiGraph:  # type: ignore[no-any-unimported]
        with open(filepath, "r") as f:
            serialised = json.load(f)
            tiktok_graph = nx.node_link_graph(data=serialised)

            logger.info(f"Loaded a {type(tiktok_graph)} object from {filepath}")

            return tiktok_graph
