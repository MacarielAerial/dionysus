import os
from datetime import datetime
from logging import Logger
from pprint import pformat
from typing import Any, Dict

from dotenv import load_dotenv
from pandas import DataFrame
from TikTokApi import TikTokApi

from ..nodes.utils import return_gen_randint


def log_hashtag_videos(hashtag: str, n_video: int, logger: Logger) -> None:
    # Task Processing
    df = _log_hashtag_videos(hashtag=hashtag, n_video=n_video, logger=logger)

    logger.info(df)


def _log_hashtag_videos(  # type: ignore[no-any-unimported]
    hashtag: str, n_video: int, logger: Logger
) -> DataFrame:
    # Load environmental variables
    load_dotenv()

    # Get verify fp value
    verifyFp = os.environ.get("S_V_WEB_ID")

    # Roughly mark the datetime of data collection
    now = datetime.now()

    logger.info(f"Datetime of data collection is marked as {now}")

    # Initiate a random number generator for request wait time
    randint_gen = return_gen_randint(start=1, end=10, n_iter=n_video + 30)

    logger.info(
        f"Initiating collection of top {n_video} videos " f"of hashtag '{hashtag}'..."
    )

    with TikTokApi(
        custom_verify_fp=verifyFp, force_verify_fp_on_cookie_header=True, logger=logger
    ) as api:
        logger.info(f"Querying hashtag '{hashtag}'")
        tag = api.hashtag(name=hashtag)
        tag_info = tag.info(count=n_video, request_delay=next(randint_gen))
        logger.info(
            f"Hastag '{hashtag}' has the following info:\n" f"{pformat(tag_info)}"
        )

        logger.info(f"Iterating over videos of hastag {hashtag}...")

        video_gen = tag.videos(count=n_video, request_delay=next(randint_gen))
        n_video = 0
        for video in video_gen:
            logger.info(f"Iterating over {n_video}th video")
            video_info: Dict[str, Any] = video.info(request_delay=next(randint_gen))
            logger.debug(f"Video ID: \n{video_info['id']}")
            logger.debug(f"Video Description:\n{video_info['desc']}")
            logger.debug(f"Time of Creation:\n{video_info['createTime']}")
            logger.debug(f"Video Statistics:\n{pformat(video.stats)}")
            logger.debug(f"Video Author:\n{video.author}")
            logger.debug(f"Video Sound Title:\n{video.sound.title}")
            logger.debug(f"Video Sound Author:\n{video.sound.author}")
            logger.debug(f"Video Hashtags:\n{video.hashtags}")
            logger.debug(f"Video Info:\n{pformat(video_info)}")

            n_video += 1

    logger.info(f"Complete parsing TikTok videos of hastag '{hashtag}'")


if __name__ == "__main__":
    import argparse
    import logging

    from ..nodes.base_logger import get_base_logger

    parser = argparse.ArgumentParser(
        description="Scrap top n TikTok videos under a specified hashtag "
        "and log responses"
    )

    # Disable request library's dependency's logging to reduce verbosity
    logging.getLogger("urllib3").setLevel(logging.WARNING)
    logger = get_base_logger()

    parser.add_argument(
        "--hashtag",
        type=str,
        required=True,
        help="The hashtag whose video information is to be downloaded",
    )
    parser.add_argument(
        "--n_video",
        type=int,
        required=False,
        default=100,
        help="The number of videos whose information is to be downloaded",
    )

    args = parser.parse_args()

    log_hashtag_videos(hashtag=args.hashtag, n_video=args.n_video, logger=logger)
