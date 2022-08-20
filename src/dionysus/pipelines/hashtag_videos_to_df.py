from datetime import datetime
from logging import Logger
from pathlib import Path
from pprint import pformat
from typing import Any, Dict

from pandas import DataFrame
from TikTokApi import TikTokApi


def curr_hashtag_videos_to_df(
    hashtag: str, n_video: int, path_df: Path, logger: Logger
) -> None:
    # Task Processing
    df = _curr_hashtag_videos_to_df(
        hashtag=hashtag, n_video=n_video, path_df=path_df, logger=logger
    )

    logger.info(df)


def _curr_hashtag_videos_to_df(  # type: ignore[no-any-unimported]
    hashtag: str, n_video: int, path_df: Path, logger: Logger
) -> DataFrame:
    logger.info(f"Collecting top {n_video} videos of hashtag '{hashtag}'...")

    # Roughly mark the datetime of data collection
    now = datetime.now()

    logger.info(f"Datetime of data collection is marked as {now}")

    with TikTokApi() as api:
        logger.info(f"Querying hashtag '{hashtag}'")
        tag = api.hashtag(name=hashtag)
        logger.info(
            f"Hastag '{hashtag}' has the following info:\n" f"{pformat(tag.info())}"
        )

        logger.info(f"Iterating over videos of hastag {hashtag}...")

        list_video = list(tag.videos(count=n_video))
        for i, video in enumerate(list_video):
            logger.info(f"Iterating over {i}th video")
            dict_info: Dict[str, Any] = video.info()
            logger.debug(f"Video ID: \n{video.id}")
            logger.debug(f"Video Description:\n{dict_info['desc']}")
            logger.debug(f"Time of Creation:\n{dict_info['createTime']}")
            logger.debug(f"Video Statistics:\n{pformat(video.stats)}")
            logger.debug(f"Video Author:\n{video.author}")
            logger.debug(f"Video Sound Title:\n{video.sound.title}")
            logger.debug(f"Video Sound Author:\n{video.sound.author}")
            logger.debug(f"Video Hashtags:\n{video.hashtags}")
            logger.debug(f"Video Info:\n{pformat(video.info())}")

    logger.info(f"Complete parsing TikTok videos of hastag '{hashtag}'")


if __name__ == "__main__":
    import argparse
    import logging

    from ..nodes.base_logger import get_base_logger

    parser = argparse.ArgumentParser(
        description="Scrap top n TikTok videos under a specified hashtag "
        "and store responses in a dataframe"
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
    parser.add_argument(
        "--path_df",
        type=Path,
        required=True,
        help="Path to a pandas dataframe to store hashtag video information",
    )

    args = parser.parse_args()

    curr_hashtag_videos_to_df(
        hashtag=args.hashtag, n_video=args.n_video, path_df=args.path_df, logger=logger
    )
