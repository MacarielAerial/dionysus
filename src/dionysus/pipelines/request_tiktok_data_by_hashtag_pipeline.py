import asyncio
import os
from pathlib import Path

from dionysus.data_interfaces.tiktok_data_interface import (
    TikTokData,
    TikTokDataInterface,
)
from dionysus.data_interfaces.tiktok_graph_data_interface import Hashtag
from dionysus.nodes.browser_utils import get_browser_params
from dionysus.nodes.data_requests import (
    form_hashtag_url,
    form_hashtag_videos_urls,
    request_hashtag_videos_data,
    request_json_data,
)


def _request_tiktok_data_by_hashtag_pipeline(challenge_name: str) -> TikTokData:
    # Retrieve authentication token from environmental variables
    ms_token = os.environ["MSTOKEN"]

    # Obtain a part of request headers from an actual browser session
    browser_params = asyncio.run(get_browser_params())

    # Form the hashtag url
    hashtag_url = form_hashtag_url(challenge_name=challenge_name, ms_token=ms_token)

    # Request data associated with the hashtag
    hashtag_data = request_json_data(url=hashtag_url, browser_params=browser_params)

    # Parse requested data into its typed presentation
    hashtag = Hashtag.from_dict(dict_response=hashtag_data)

    # Form hashtag videos urls
    hashtag_videos_urls = form_hashtag_videos_urls(
        hashtag_id=hashtag.id, ms_token=ms_token
    )

    # Request data associated with videos of the hashtag
    list_video_data = request_hashtag_videos_data(
        hashtag_videos_urls=hashtag_videos_urls, browser_params=browser_params
    )

    # Compile result dataclass instance
    tiktok_data = TikTokData(hashtag_data=hashtag_data, list_video_data=list_video_data)

    return tiktok_data


def request_tiktok_data_by_hashtag_pipeline(
    challenge_name: str, path_tiktok_data: Path
) -> None:
    # Task Processing
    tiktok_data = _request_tiktok_data_by_hashtag_pipeline(
        challenge_name=challenge_name,
    )

    # Data Access - Output
    tiktok_data_interface = TikTokDataInterface(filepath=path_tiktok_data)
    tiktok_data_interface.save(data=tiktok_data)


if __name__ == "__main__":
    import argparse

    from dotenv import load_dotenv

    from dionysus.nodes.project_logging import default_logging

    default_logging()
    load_dotenv()

    parser = argparse.ArgumentParser(
        description="Request TikTok data associated with the given hashtag name"
    )
    parser.add_argument(
        "-name",
        "--challenge_name",
        type=str,
        required=True,
        help="Name of the hashtag to query to form a TikTok Graph",
    )
    parser.add_argument(
        "-ptd",
        "--path_tiktok_data",
        type=Path,
        required=True,
        help="Path to a serialised networkx graph instance which stores parsed output",
    )

    args = parser.parse_args()

    request_tiktok_data_by_hashtag_pipeline(
        challenge_name=args.challenge_name, path_tiktok_data=args.path_tiktok_data
    )
