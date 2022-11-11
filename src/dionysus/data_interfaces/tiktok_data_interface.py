import logging
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List

import orjson
from networkx import DiGraph

from dionysus.data_interfaces.tiktok_graph_data_interface import (
    Hashtag,
    NodeAttrKey,
    VideoSubGraph,
)
from dionysus.nodes.graph_operations import contract_graphs_by_node_attr

logger = logging.getLogger(__name__)


@dataclass
class TikTokData:
    hashtag_data: Dict[str, Any]
    """
    {
        "coverLarger": "",
        "coverMedium": "",
        "coverThumb": "",
        "desc": "",
        "id": "18478",
        "isCommerce": false,
        "profileLarger": "",
        "profileMedium": "",
        "profileThumb": "",
        "stats": {
            "videoCount": 0,
            "viewCount": 0
        },
        "title": "zouk"
        }
    """
    list_video_data: List[Dict[str, Any]]
    """
    {
        "adAuthorization": false,
        "adLabelVersion": 0,
        "author": {
                ...
            },
        "authorStats": {
            "diggCount": 126,
            "followerCount": 13800,
            "followingCount": 90,
            "heart": 128200,
            "heartCount": 128200,
            "videoCount": 197
        },
        "challenges": [
            {
                ...
            }
        ],
        "createTime": 1630508846,
        "desc": "Bora girar...,
        "digged": false,
        "duetDisplay": 0,
        "duetEnabled": true,
        "duetInfo": {
            "duetFromId": "0"
        },
        "forFriend": false,
        "id": "7002982165919157509",
        "isAd": false,
        "itemCommentStatus": 0,
        "itemMute": false,
        "music": {
            ...
        },
        "officalItem": false,
        "originalItem": false,
        "privateItem": false,
        "secret": false,
        "shareEnabled": true,
        "showNotPass": false,
        "stats": {
            "commentCount": 1821,
            "diggCount": 90100,
            "playCount": 615200,
            "shareCount": 1082
        },
        "stitchDisplay": 0,
        "stitchEnabled": true,
        "textExtra": [
            {
            "awemeId": "",
            "end": 39,
            "hashtagId": "18478",
            "hashtagName": "zouk",
            "isCommerce": false,
            "secUid": "",
            "start": 34,
            "subType": 0,
            "type": 1,
            "userId": "",
            "userUniqueId": ""
        },
        ...
        ],
        "video": {
            "bitrate": 1047431,
            "bitrateInfo": [
            {
                "Bitrate": 1047431,
                "CodecType": "h264",
                "GearName": "normal_720_0",
                "PlayAddr": {
                "DataSize": 3192046,
                "FileCs": "c:0-21345-7a6a",
                "FileHash": "03e052d002b6db4eabf9e3f212da8a7e",
                "Uri": "v09044g40000c4npdjrc77ufk4l6khk0",
                "UrlKey": "v09044g40000c4npdjrc77ufk4l6khk0_h264_720p_1047431",
                "UrlList": [
                    "...",
                    "..."
                ]
                },
                "QualityType": 10
            }
            ],
            "codecType": "h264",
            "cover": "...",
            "definition": "720p",
            "downloadAddr": "...",
            "duration": 24,
            "dynamicCover": "...",
            "encodeUserTag": "",
            "encodedType": "normal",
            "format": "mp4",
            "height": 1024,
            "id": "7002982165919157509",
            "originCover": "...",
            "playAddr": "...",
            "ratio": "720p",
            "reflowCover": "...",
            "shareCover": [
            "",
            "...",
            "..."
            ],
            "videoQuality": "normal",
            "volumeInfo": {
            "Loudness": -22.5,
            "Peak": 0.53088
            },
            "width": 576,
            "zoomCover": {
            "240": "...",
            "480": "...",
            "720": "...",
            "960": "..."
            }
        },
        "vl1": false
    }
    """

    def to_tiktok_graph(self) -> DiGraph:  # type: ignore[no-any-unimported]
        # Parse hashtag data
        hashtag = Hashtag.from_dict(dict_response=self.hashtag_data)

        # Parse video data
        list_video_subgraph: List[VideoSubGraph] = []
        for video_data in self.list_video_data:
            video_subgraph = VideoSubGraph.from_dict(dict_info=video_data)
            list_video_subgraph.append(video_subgraph)

        logger.info(
            f"Parsed {len(list_video_subgraph)} {VideoSubGraph} instances in total "
            f"which are associated with hashtag {hashtag.title} whose id is "
            f"{hashtag.id}"
        )

        # Join video subgraph-lets together
        tiktok_graph = contract_graphs_by_node_attr(
            *[video_subgraph.nx_g for video_subgraph in list_video_subgraph],
            node_attr_key=NodeAttrKey.id,
        )

        return tiktok_graph


class TikTokDataInterface:
    def __init__(self, filepath: Path) -> None:
        self.filepath = filepath

    def save(self, data: TikTokData) -> None:
        self._save(filepath=self.filepath, data=data)

    @staticmethod
    def _save(filepath: Path, data: TikTokData) -> None:
        with open(filepath, "wb") as f:
            serialised = orjson.dumps(data)
            f.write(serialised)

            logger.info(f"Saved a {type(data)} object to {filepath}")

    def load(self) -> TikTokData:
        return self._load(filepath=self.filepath)

    @staticmethod
    def _load(filepath: Path) -> TikTokData:
        with open(filepath, "rb") as f:
            serialised = orjson.loads(f.read())

            tiktok_data = TikTokData(
                hashtag_data=serialised["hashtag_data"],
                list_video_data=serialised["list_video_data"],
            )

            logger.info(f"Loaded a {type(tiktok_data)} object from {filepath}")

            return tiktok_data
