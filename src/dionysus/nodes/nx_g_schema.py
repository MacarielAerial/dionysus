from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import Any, Dict, List, Tuple

import pandas as pd
from pandas import DataFrame, Series

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

    # Attributes specific to video and sound
    duration: str = "duration"

    # Attributes specific to author and hashtag
    video_count: str = "video_count"


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
        node_attrs_native: Dict[str, Any] = {}

        for node_attr in self.list_node_attr:
            node_attrs_native.update(node_attr.to_dict_native())

        return node_attrs_native

    def to_multi_index_dict_native(self) -> Dict[Tuple[str, str], Any]:
        multi_index_dict_native: Dict[Tuple[str, str], Any] = {}

        node_attrs_native = self.to_dict_native()

        ntype = node_attrs_native.pop(NodeAttrKey.ntype.value)

        for node_attr_key, node_attr_val in node_attrs_native.items():
            multi_index_dict_native.update({(ntype, node_attr_key): node_attr_val})

        return multi_index_dict_native

    def to_df(self) -> DataFrame:  # type: ignore[no-any-unimported]
        multi_index_dict_native = self.to_multi_index_dict_native()

        s = Series(
            multi_index_dict_native.values(),
            index=pd.MultiIndex.from_tuples(
                multi_index_dict_native.keys(), names=["ntype", "nfeat"]
            ),
            name=0,
        )

        df = (s.to_frame()).T

        return df


@dataclass
class NodeTuple:
    nid: int
    node_attrs: NodeAttrs

    def to_node_tuple_native(self) -> Tuple[int, Dict[str, Any]]:
        return (self.nid, self.node_attrs.to_dict_native())

    @classmethod
    def from_node_series(  # type: ignore[no-any-unimported]
        cls, nid: int, node_series: Series
    ) -> NodeTuple:
        node_tuple = NodeTuple(nid=nid, node_attrs=NodeAttrs(list_node_attr=[]))

        for node_attr_key, node_attr_val in node_series.iteritems():
            node_attr = NodeAttr(
                node_attr_key=NodeAttrKey(node_attr_key), node_attr_val=node_attr_val
            )
            node_tuple.node_attrs.list_node_attr.append(node_attr)

        return node_tuple


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
    music_to_video: str = "MusicToVideo"


class EdgeAttrKey(Enum):
    etype: str = "etype"
    src_original_id: str = "src_original_id"
    dst_original_id: str = "dst_original_id"


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
    list_edge_attr: List[EdgeAttr]

    def to_dict_native(self) -> Dict[str, Any]:
        edge_attrs_native: Dict[str, Any] = {}

        for edge_attr in self.list_edge_attr:
            edge_attrs_native.update(edge_attr.to_dict_native())

        return edge_attrs_native

    def to_multi_index_dict_native(self) -> Dict[Tuple[str, str], Any]:
        multi_index_dict_native: Dict[Tuple[str, str], Any] = {}

        edge_attrs_native = self.to_dict_native()

        etype = edge_attrs_native.pop(EdgeAttrKey.etype.value)

        for edge_attr_key, edge_attr_val in edge_attrs_native.items():
            multi_index_dict_native.update({(etype, edge_attr_key): edge_attr_val})

        return multi_index_dict_native

    def to_df(self) -> DataFrame:  # type: ignore[no-any-unimported]
        multi_index_dict_native = self.to_multi_index_dict_native()

        s = Series(
            multi_index_dict_native.values(),
            index=pd.MultiIndex.from_tuples(
                multi_index_dict_native.keys(), names=["etype", "efeat"]
            ),
            name=0,
        )

        df = (s.to_frame()).T

        return df


@dataclass
class EdgeTuple:
    src_nid: int
    dst_nid: int
    edge_attrs: EdgeAttrs

    def to_edge_tuple_native(self) -> Tuple[int, int, Dict[str, Any]]:
        return (self.src_nid, self.dst_nid, self.edge_attrs.to_dict_native())

    @classmethod
    def from_edge_series(  # type: ignore[no-any-unimported]
        cls, src_nid: int, dst_nid: int, edge_series: Series
    ) -> EdgeTuple:
        edge_tuple = EdgeTuple(
            src_nid=src_nid, dst_nid=dst_nid, edge_attrs=EdgeAttrs(list_edge_attr=[])
        )

        for edge_attr_key, edge_attr_val in edge_series.iteritems():
            edge_attr = EdgeAttr(
                edge_attr_key=EdgeAttrKey(edge_attr_key), edge_attr_val=edge_attr_val
            )
            edge_tuple.edge_attrs.list_edge_attr.append(edge_attr)

        return edge_tuple


@dataclass
class EdgeTuples:
    list_edge_tuple: List[EdgeTuple]

    def to_list_edge_tuple_native(self) -> List[Tuple[int, int, Dict[str, Any]]]:
        return [
            edge_tuple.to_edge_tuple_native() for edge_tuple in self.list_edge_tuple
        ]
