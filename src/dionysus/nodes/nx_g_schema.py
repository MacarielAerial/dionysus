from dataclasses import dataclass
from enum import Enum
from typing import Any, Dict, List, Tuple

#
# Node Related
#


class NodeType(Enum):
    video: str = "VIDEO"
    hashtag: str = "HASHTAG"
    author: str = "AUTHOR"
    music: str = "MUSIC"


class NodeAttrKey(Enum):
    # Common attributes across all node types
    id: str = "id"
    ntype: str = "ntype"
    text: str = "text"

    # Attributes specific to video
    width: str = "width"
    height: str = "height"
    definition: str = "definition"
    format: str = "format"
    comment_count: str = "comment_count"
    play_count: str = "play_count"
    share_count: str = "share_count"

    # Attributes specific to hashtag
    view_count: str = "viewCount"

    # Attributes specific to author
    nickname: str = "nickname"
    is_private_account: str = "is_private_account"
    signature: str = "signature"
    verified: str = "verified"
    follower_count: str = "follower_count"
    following_count: str = "following_count"
    heart: str = "heart"

    # Attributes specific to sound
    album: str = "album"
    author_name: str = "author_name"

    play_url: str = "play_url"

    # Attributes specific to video and author
    digg_count: str = "digg_count"

    # Attributes specific to video and hastag
    creation_time: str = "creation_time"
    video_count: str = "video_count"

    # Attributes specific to video and sound
    duration: str = "duration"


@dataclass
class NodeAttr:
    node_attr_key: NodeAttrKey
    node_attr_val: Any

    def to_dict_native(self) -> Dict[str, Any]:
        if isinstance(self.node_attr_val, Enum):
            return {self.node_attr_key.value: self.node_attr_val.value}
        else:
            return {self.node_attr_key.value: self.node_attr_val}


@dataclass
class NodeAttrs:
    list_node_attr: List[NodeAttr]

    def to_dict_native(self) -> Dict[str, Any]:
        dict_native: Dict[str, Any] = {}

        for node_attr in self.list_node_attr:
            dict_native.update(node_attr.to_dict_native())

        return dict_native


@dataclass
class NodeTuple:
    nid: int
    node_attrs: NodeAttrs

    def to_node_tuple_native(self) -> Tuple[int, Dict[str, Any]]:
        return (self.nid, self.node_attrs.to_dict_native())


@dataclass
class NodeTuples:
    list_node_tuple: List[NodeTuple]

    def to_list_node_tuple_native(self) -> List[Tuple[int, Dict[str, Any]]]:
        return [
            node_tuple.to_node_tuple_native() for node_tuple in self.list_node_tuple
        ]


#
# Edge Related
#


class EdgeType(Enum):
    video_to_hashtag: str = "VideoToHashtag"
    author_to_video: str = "AuthorToVideo"
    sound_to_video: str = "SoundToVideo"


class EdgeAttrKey(Enum):
    etype: str = "etype"


@dataclass
class EdgeAttr:
    edge_attr_key: EdgeAttrKey
    edge_attr_val: Any

    def to_dict_native(self) -> Dict[str, Any]:
        if isinstance(self.edge_attr_val, Enum):
            return {self.edge_attr_key.value: self.edge_attr_val.value}
        else:
            return {self.edge_attr_key.value: self.edge_attr_val}


@dataclass
class EdgeAttrs:
    list_edge_attr: List[NodeAttr]

    def to_dict_native(self) -> Dict[str, Any]:
        dict_native: Dict[str, Any] = {}

        for edge_attr in self.list_edge_attr:
            dict_native.update(edge_attr.to_dict_native())

        return dict_native


@dataclass
class EdgeTuple:
    src_nid: int
    dst_nid: int
    edge_attrs: EdgeAttrs

    def to_edge_tuple_native(self) -> Tuple[int, int, Dict[str, Any]]:
        return (self.src_nid, self.dst_nid, self.edge_attrs.to_dict_native())


@dataclass
class EdgeTuples:
    list_edge_tuple: List[EdgeTuple]

    def to_list_edge_tuple_native(self) -> List[Tuple[int, int, Dict[str, Any]]]:
        return [
            edge_tuple.to_edge_tuple_native() for edge_tuple in self.list_edge_tuple
        ]
