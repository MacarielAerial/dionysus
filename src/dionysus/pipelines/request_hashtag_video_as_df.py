import os
from datetime import datetime
from logging import Logger
from pathlib import Path
from pprint import pformat
from typing import Any, Dict, List

import pandas as pd
from dotenv import load_dotenv
from pandas import DataFrame, Series
from TikTokApi import TikTokApi

from ..datasets.pandas_hdf5_dataset import PandasHDF5DataSet
from ..nodes.utils import return_gen_randint


def request_hashtag_video_as_df(
    hashtag: str, n_video: int, path_df: Path, logger: Logger
) -> None:
    # Task Processing
    df = _request_hashtag_video_as_df(hashtag=hashtag, n_video=n_video, logger=logger)

    # Data Access - Output
    pandas_hdf5_dataset = PandasHDF5DataSet(filepath=path_df, logger=logger)
    pandas_hdf5_dataset.save(df=df)


def _request_hashtag_video_as_df(  # type: ignore[no-any-unimported]
    hashtag: str, n_video: int, logger: Logger
) -> DataFrame:
    # Initiate intermediate variable
    list_single_col_df: List[DataFrame] = []  # type: ignore[no-any-unimported]

    # Load environmental variables
    load_dotenv()

    # Get verify fp value
    verifyFp = os.environ.get("S_V_WEB_ID")

    # Roughly mark the datetime of data collection
    now = datetime.now()

    logger.info(f"Datetime of request is marked as {now}")

    # Initiate a random number generator for request wait time
    randint_gen = return_gen_randint(start=1, end=10, n_iter=n_video + 30)

    logger.info(f"Querying hashtag '{hashtag}'...")

    with TikTokApi(
        custom_verify_fp=verifyFp, force_verify_fp_on_cookie_header=True, logger=logger
    ) as api:
        api_hashtag = api.hashtag(name=hashtag)
        hashtag_info = api_hashtag.info(request_delay=next(randint_gen))

        logger.debug(
            f"Hastag '{hashtag}' has the following info:\n" f"{pformat(hashtag_info)}"
        )

        # Parse hashtag data into a dictionary
        dict_hashtag_info: Dict[str, Any] = {
            "hashtag_id": hashtag_info["id"],
            "hashtag_title": hashtag_info["title"],
            "hashtag_view_count": hashtag_info["stats"]["viewCount"],
            "hashtag_video_count": hashtag_info["stats"]["videoCount"],
        }

        video_gen = api_hashtag.videos(count=n_video, request_delay=next(randint_gen))
        n_video = 0
        for video in video_gen:
            logger.info(f"Iterating over {n_video}th video...")

            video_info: Dict[str, Any] = video.info(request_delay=next(randint_gen))
            video_stats = video_info["stats"]
            author_info = video_info["author"]
            author_stats = video_info["authorStats"]
            music_info = video_info["music"]

            # Parse video data into a pandas series
            s = Series(
                {
                    "video_id": video_info["id"],
                    "video_description": video_info["desc"],
                    "video_create_time": video_info["createTime"],
                    "video_duration": video_info["video"]["duration"],
                    "video_width": video_info["video"]["width"],
                    "video_height": video_info["video"]["height"],
                    "video_definition": video_info["video"]["definition"],
                    "video_format": video_info["video"]["format"],
                    "video_comment_count": video_stats["commentCount"],
                    "video_play_count": video_stats["playCount"],
                    "video_share_count": video_stats["shareCount"],
                    "video_digg_count": video_stats["diggCount"],
                    "author_id": author_info["id"],
                    "author_unique_id": author_info["uniqueId"],
                    "author_nickname": author_info["nickname"],
                    "author_is_private_account": author_info["privateAccount"],
                    "author_signature": author_info["signature"],
                    "author_verified": author_info["verified"],
                    "author_follower_count": author_stats["followerCount"],
                    "author_following_count": author_stats["followingCount"],
                    "author_heart": author_stats["heart"],
                    "author_digg_count": author_stats["diggCount"],
                    "author_video_count": author_stats["videoCount"],
                    "music_id": music_info["id"],
                    "music_title": music_info["title"],
                    "music_author_name": music_info["authorName"],
                    "music_album": music_info["album"],
                    "music_duration": music_info["duration"],
                    "music_play_url": music_info["playUrl"],
                },
                name=n_video,
            )

            n_video += 1

            # Persist parsed series
            list_single_col_df.append(s.to_frame().T)

        logger.info(
            f"Completed parsing API responses into {len(list_single_col_df)} series"
        )

    # Compile list of series into a dataframe
    df = pd.concat(list_single_col_df)
    df.attrs.update(dict_hashtag_info)
    df.attrs.update({"datetime_request": now.isoformat()})

    logger.info(
        f"Complete parsing TikTok videos of hastag '{api_hashtag}' "
        "into a dataframe whose information is as followed:\n"
        f"{df.describe()}"
    )
    logger.info(f"Here's a sample of its content:\n{df.head()}")

    return df


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

    request_hashtag_video_as_df(
        hashtag=args.hashtag, n_video=args.n_video, path_df=args.path_df, logger=logger
    )
