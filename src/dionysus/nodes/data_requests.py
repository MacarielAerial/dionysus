import logging
import time
from pprint import pformat
from typing import Any, Dict, List
from urllib.parse import urlencode

import requests

from dionysus.conf_default.globals import (
    CHALLENGE_SUB_URL,
    CHALLENGE_VIDEOS_SUB_URL,
    ROOT_URL,
)
from dionysus.nodes.browser_utils import BrowserParams
from dionysus.nodes.utils import int_from_discrete_gaussian_dist

logger = logging.getLogger(__name__)


def request_json_data(url: str, browser_params: BrowserParams) -> Dict[str, Any]:
    #
    # Execute delay
    #

    request_delay = int_from_discrete_gaussian_dist()

    logger.debug(f"Schdueld to wait for {request_delay} seconds")

    time.sleep(request_delay)

    #
    # Form headers
    #

    headers = {
        "authority": "m.tiktok.com",
        "method": "GET",
        "path": url.split("tiktok.com")[1],
        "scheme": "https",
        "accept": "application/json, text/plain, */*",
        "accept-encoding": "gzip",
        "accept-language": "en-US,en;q=0.9",
        "origin": ROOT_URL,
        "referer": ROOT_URL,
        "user-agent": browser_params.user_agent,
    }

    #
    # Execute the request
    #

    r = requests.get(
        url,
        headers=headers,
    )

    json_data = r.json()

    if json_data["status_code"] != 0:
        raise ValueError(
            f"JSON query returned non-zero status code:\n{json_data['status_code']}"
        )
    else:
        logger.debug(
            f"The request to path {url} with the following headers:\n"
            f"{pformat(headers)}\nreturns json data with the following keys:\n"
            f"{pformat(json_data.keys())}"
        )

        return json_data  # type: ignore[no-any-return]


def form_hashtag_url(challenge_name: str, ms_token: str) -> str:
    # Compile the query dictionary
    query = {"challengeName": challenge_name, "msToken": ms_token}
    # Inject the query dictionary into the hashtag url template
    challenge_sub_url = CHALLENGE_SUB_URL.format(urlencode(query))
    # Form the complete url
    hashtag_url = ROOT_URL + challenge_sub_url

    logger.debug(f"Formed a hashtag url:\n{hashtag_url}")

    return hashtag_url


def form_hashtag_videos_url(hashtag_id: int, cursor: int, ms_token: str) -> str:
    # Configure the query dictionary
    query = {
        "aid": 1988,
        "count": 30,
        "challengeID": hashtag_id,
        "cursor": cursor,
        "msToken": ms_token,
    }
    # Inject the query dictionary into the hashtag video url template
    challenge_videos_sub_url = CHALLENGE_VIDEOS_SUB_URL.format(urlencode(query))

    # Form the complete url
    hashtag_videos_url = ROOT_URL + challenge_videos_sub_url

    logger.debug(f"Formed a challenge videos sub url:\n{hashtag_videos_url}")

    return hashtag_videos_url


def form_hashtag_videos_urls(hashtag_id: int, ms_token: str) -> List[str]:
    hashtag_videos_urls: List[str] = []

    cursor: int = 0
    while cursor < 5000:  # An arbitrarily large number
        hashtag_videos_url = form_hashtag_videos_url(
            hashtag_id=hashtag_id, cursor=cursor, ms_token=ms_token
        )
        hashtag_videos_urls.append(hashtag_videos_url)

        cursor = cursor + 30

    return hashtag_videos_urls


def request_hashtag_videos_data(
    hashtag_videos_urls: List[str], browser_params: BrowserParams
) -> List[Dict[str, Any]]:
    # Initiate an iterable for contain json data of all hashtag videos
    list_video_json_data: List[Dict[str, Any]] = []

    # Initiate a marker for the end of the loop
    has_more: bool = True

    # Initiate a marker to track the number of requests completed
    n_completed: int = 0

    while has_more:
        hashtag_videos_json_data = request_json_data(
            url=hashtag_videos_urls[n_completed], browser_params=browser_params
        )

        # Check if the end loop condition is satisfied
        has_more = bool(hashtag_videos_json_data["hasMore"])

        if has_more:
            logger.debug(
                f"TikTok indicates more videos are available "
                f"after the {n_completed}th request"
            )

            n_completed += 1

            for video_json_data in hashtag_videos_json_data["itemList"]:
                # Transplant timestamp data from the videos level to the video level
                video_json_data.update(
                    {"now": hashtag_videos_json_data["extra"]["now"]}
                )
                list_video_json_data.append(video_json_data)
        else:
            logger.info(
                "Tiktok indicates no more videos are available "
                f"after the {n_completed}th request"
            )
            break

    return list_video_json_data
