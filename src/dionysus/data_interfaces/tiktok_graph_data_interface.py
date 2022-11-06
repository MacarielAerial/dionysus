from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
import logging
import json
from pathlib import Path
from datetime import datetime
from typing import Any, Dict, Optional

import networkx as nx
from networkx import DiGraph

logger = logging.getLogger(__name__)


class NodeAttrKey(Enum):
    # Hashtag
    ntype: str = 'ntype'
    id: str = 'id'
    title: str = 'title'
    view_count: str = 'view_count'
    video_count: str = 'video_count'
    timestamp: str = 'timestamp'
    
    # Union with Video
    desc: str = 'desc'
    create_time: str = 'create_time'
    duration: str = 'duration'
    width: str = 'width'
    height: str = 'height'
    defintion: str = 'definition'
    format: str = 'format'
    comment_count: str = 'comment_count'
    play_count: str = 'play_count'
    share_count: str = 'share_count'
    digg_count: str = 'digg_count'
    
    # Union with Author
    unique_id: str = 'unique_id'
    nickname: str = 'nickname'
    is_priviate_account: str = 'is_private_account'
    signature: str = 'signature'
    verified: str = 'verified'
    follower_count: str = 'follower_count'
    following_count: str = 'following_count'
    heart: str = 'heart'
    
    # Union with Music
    author_name: str
    album: str
    duration: str
    play_url: str


class NodeType(Enum):
    hashtag: str = 'Hashtag'
    video: str = 'Video'
    author: str = 'Author'
    music: str = 'Music'


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
        extra = dict_response['extra']
        challenge_info = dict_response['challengeInfo']
        challenge = challenge_info['challenge']
        stats = challenge['stats']
        
        # Parse each node attribute
        ntype = NodeType.hashtag
        id = int(challenge['id'])
        title = str(challenge['title'])
        view_count = int(stats['viewCount'])
        video_count = int(stats['videoCount'])
        timestamp = int(extra['now'])
        
        # Assemble the dataclass instance
        hashtag = cls(
            ntype=ntype,
            id=id,
            title=title,
            view_count=view_count,
            video_count=video_count,
            timestamp=timestamp
        )
        
        return hashtag
    
    def to_node_attrs(self) -> Dict[str, Any]:
        return {
            NodeAttrKey.ntype.value: self.ntype.value,
            NodeAttrKey.id.value: self.id,
            NodeAttrKey.title.value: self.title,
            NodeAttrKey.view_count.value: self.view_count,
            NodeAttrKey.video_count.value: self.video_count,
            NodeAttrKey.timestamp.value: self.timestamp
        }


@dataclass
class Video:
    ntype: NodeType
    id: str
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
        id = str(dict_info['id'])
        desc = str(dict_info['desc'])
        create_time = int(dict_info['createTime'])
        duration = str(dict_info['video']['duration'])
        width = int(dict_info['video']['width'])
        height = int(dict_info['video']['height'])
        definition = str(dict_info['video'].get('definition', None))
        format = str(dict_info['video'].get('format', None))
        comment_count = int(dict_info['stats']['commentCount'])
        play_count = int(dict_info['stats']['playCount'])
        share_count = int(dict_info['stats']['shareCount'])
        digg_count = int(dict_info['stats']['diggCount'])
        time_stamp = int(dict_info['now'])
        
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
            time_stamp=time_stamp
        )
        
        return video
    
    def to_node_attrs(self) -> Dict[str, Any]:
        return {
            NodeAttrKey.ntype.value: self.ntype.value,
            NodeAttrKey.id.value: self.id,
            NodeAttrKey.desc.value: self.desc,
            NodeAttrKey. create_time: self.create_time,
            NodeAttrKey.duration: self.duration,
            NodeAttrKey.width: self.width,
            NodeAttrKey.height: self.height,
            NodeAttrKey.defintion: self.definition,
            NodeAttrKey.format: self.format,
            NodeAttrKey.comment_count: self.comment_count,
            NodeAttrKey.play_count: self.play_count,
            NodeAttrKey.share_count: self.share_count,
            NodeAttrKey.digg_count: self.digg_count,
            NodeAttrKey.timestamp: self.time_stamp
        }


@dataclass
class Author:
    ntype: NodeType
    id: str
    unique_id: str
    nickname: str
    is_priviate_account: bool
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
        id = str(dict_info['id'])
        unique_id = str(dict_info['uniqueId'])
        nickname = str(dict_info['nickname'])
        is_private_account = bool(dict_info['privateAccount'])
        signature = str(dict_info['signature'])
        verified = bool(dict_info['verified'])
        follower_count = int(dict_info['stats']['followerCount'])
        following_count = int(dict_info['stats']['followingCount'])
        heart = int(dict_info['stats']['heart'])
        digg_count = int(dict_info['stats']['diggCount'])
        video_count = int(dict_info['stats']['videoCount'])
    
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
            video_count=video_count
        )
    
        return author

    def to_node_attrs(self) -> Dict[str, Any]:
        return {
            NodeAttrKey.ntype: self.ntype.value,
            NodeAttrKey.id: self.id,
            NodeAttrKey.unique_id: self.unique_id,
            NodeAttrKey.nickname: self.nickname,
            NodeAttrKey.is_priviate_account: self.is_priviate_account,
            NodeAttrKey.signature: self.signature,
            NodeAttrKey.verified: self.verified,
            NodeAttrKey.follower_count: self.follower_count,
            NodeAttrKey.following_count: self.following_count,
            NodeAttrKey.heart: self.heart,
            NodeAttrKey.digg_count: self.digg_count,
            NodeAttrKey.video_count: self.video_count,
        }


@dataclass
class Music:
    ntype: NodeType
    id: str
    title: str
    author_name: str
    album: str
    duration: str
    play_url: str
    
    @classmethod
    def from_dict(cls, dict_info: Dict[str, Any]) -> Music:
        # Parse each node attribute
        ntype = NodeType.music.value
        id = dict_info['id']
        title = dict_info['title']
        author_name = dict_info['authorName']
        album = dict_info['album']
        duration = dict_info['duration']
        play_url = dict_info['playUrl']
        
        # Assemble the dataclass instance
        music = cls(
            ntype=ntype,
            id=id,
            title=title,
            author_name=author_name,
            album=album,
            duration=duration,
            play_url=play_url
        )
        
        return music
 

class TikTokGraphDataInterface:
    def __init__(self, filepath: Path) -> None:
        self.filepath = filepath
    
    def save(self, data: DiGraph) -> None:
        self._save(filepath=self.filepath, tiktok_graph=data)
    
    @staticmethod
    def _save(filepath: Path, tiktok_graph: DiGraph) -> None:
        with open(filepath, 'w') as f:
            serialised = nx.node_link_data(G=tiktok_graph)
            json.dump(serialised, f)
            
            logger.info(f'Saved a {type(tiktok_graph)} object to {filepath}')
        
    def load(self) -> DiGraph:
        return self._load(filepath=self.filepath)
    
    @staticmethod
    def _load(filepath: Path) -> DiGraph:
        with open(filepath, 'r') as f:
            serialised = json.load(f)
            tiktok_graph = nx.node_link_graph(data=serialised)
            
            logger.info(f'Loaded a {type(tiktok_graph)} object from {filepath}')
            
            return tiktok_graph
