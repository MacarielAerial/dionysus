import logging
import os
import asyncio
from typing import List
from dotenv import load_dotenv

from networkx import DiGraph

from dionysus.nodes.browser_utils import get_browser_params
from dionysus.nodes.data_requests import form_hashtag_url, request_json_data, form_hashtag_videos_urls, request_hashtag_videos_json_data
from dionysus.data_interfaces.tiktok_graph_data_interface import Hashtag, Video

logger = logging.getLogger(__name__)


def _request_tiktok_graph_by_hashtag_pipeline(
    challenge_name: str
) -> DiGraph:
    # Retrieve authentication token from environmental variables
    ms_token = os.environ['MSTOKEN']
    
    # Obtain a part of request headers from an actual browser session
    browser_params = asyncio.run(get_browser_params())
    
    # Form the hashtag url
    hashtag_url = form_hashtag_url(challenge_name=challenge_name,
                                   ms_token=ms_token)
    
    # Request data associated with the hashtag
    hashtag_json_data = request_json_data(url=hashtag_url,
                                     browser_params=browser_params)
    
    # Parse hashtag json data returned
    hashtag = Hashtag.from_dict(dict_response=hashtag_json_data)
    
    # Form hashtag videos urls
    hashtag_videos_urls = form_hashtag_videos_urls(
        hashtag_id=hashtag.id,
        ms_token=ms_token
    )
    
    # Request data associated with videos of the hashtag
    list_video_json_data = request_hashtag_videos_json_data(
        hashtag_videos_urls=hashtag_videos_urls,
        browser_params=browser_params
    )
    
    # Parse video json data returned
    list_video: List[Video] = []
    for video_json_data in list_video_json_data:
        video = Video.from_dict(dict_info=video_json_data)
        list_video.append(video)
    
    logger.info(f'Parsed {len(list_video)} {Video} instances in total '
                f'associated with hashtag {hashtag.title} whose id is '
                f'{hashtag.id}')


if __name__ == '__main__':
    from dotenv import load_dotenv
    import argparse
    
    from dionysus.nodes.project_logging import default_logging
    
    default_logging()
    load_dotenv()
    
    parser = argparse.ArgumentParser(
        description='Request TikTok data associated with the given hashtag name'
    )
    parser.add_argument(
        '-name',
        '--challenge_name',
        type=str,
        required=True,
        help='Name of the hashtag to query to form a TikTok Graph'
    )
    
    args = parser.parse_args()
    
    _request_tiktok_graph_by_hashtag_pipeline(
        challenge_name=args.challenge_name
    )
