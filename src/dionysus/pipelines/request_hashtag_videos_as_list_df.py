import os
from datetime import datetime
from logging import Logger
from pathlib import Path
from typing import Any, Dict, List

import pandas as pd
from dotenv import load_dotenv
from pandas import DataFrame
from TikTokApi import TikTokApi

from ..datasets.tiktok_database_dataset import TikTokDataBase, TikTokDataBaseDataSet
from ..nodes.api_response_parsing import (
    author_info_stats_to_author_node_attrs,
    hashtag_info_to_hashtag_node_attrs,
    id_to_author_to_video_edge_attrs,
    id_to_music_to_video_edge_attrs,
    id_to_video_to_hashtag_edge_attrs,
    music_info_to_music_node_attrs,
    video_info_to_video_node_attrs,
)
from ..nodes.nx_g_schema import NodeAttrKey, NodeType
from ..nodes.utils import return_gen_randint


def request_hashtag_video_as_df(
    hashtag: str, n_video: int, path_df: Path, logger: Logger
) -> None:
    # Task Processing
    tiktok_database = _request_hashtag_video_as_df(
        hashtag=hashtag, n_video=n_video, logger=logger
    )

    # Data Access - Output
    tiktok_database_dataset = TikTokDataBaseDataSet(filepath=path_df, logger=logger)
    tiktok_database_dataset.save(tiktok_database=tiktok_database)


def _request_hashtag_video_as_df(
    hashtag: str, n_video: int, logger: Logger
) -> TikTokDataBase:
    # Initiate intermediate variable
    list_video_df: List[DataFrame] = []  # type: ignore[no-any-unimported]
    list_author_df: List[DataFrame] = []  # type: ignore[no-any-unimported]
    list_music_df: List[DataFrame] = []  # type: ignore[no-any-unimported]
    list_hashtag_df: List[DataFrame] = []  # type: ignore[no-any-unimported]

    list_author_to_video_df: List[DataFrame] = []  # type: ignore[no-any-unimported]
    list_music_to_video_df: List[DataFrame] = []  # type: ignore[no-any-unimported]
    list_video_to_hashtag_df: List[DataFrame] = []  # type: ignore[no-any-unimported]

    # Load environmental variables
    load_dotenv()

    # Load cookie
    msToken = os.environ.get("msToken")

    # Roughly mark the datetime of data collection
    now = datetime.now()

    logger.info(f"Datetime of request is marked as {now}")

    # Initiate a random number generator for request wait time
    randint_gen = return_gen_randint(start=10, end=20)

    logger.info(f"Querying hashtag '{hashtag}'...")

    with TikTokApi(msToken=msToken, logger=logger) as api:
        api_hashtag = api.hashtag(name=hashtag)

        video_gen = api_hashtag.videos(count=n_video, request_delay=next(randint_gen))
        n_video_response = 0
        for video in video_gen:
            # Iterate over videos under the hashtag

            logger.info(f"Iterating over {n_video_response}th video...")

            # Identify subsections of response
            video_info = video.info(request_delay=next(randint_gen))
            author_info = video_info["author"]
            author_stats = video_info["authorStats"]
            music_info = video_info["music"]
            list_hashtag_info: List[Dict[str, Any]] = video_info["challenges"]

            #
            # Node Parsing
            #

            # Parse video node data into a dataframe
            video_attrs = video_info_to_video_node_attrs(video_info=video_info)
            video_df = video_attrs.to_df()
            list_video_df.append(video_df)

            # Parse author node data into a dataframe
            author_attrs = author_info_stats_to_author_node_attrs(
                author_info=author_info, author_stats=author_stats
            )
            author_df = author_attrs.to_df()
            list_author_df.append(author_df)

            # Parse music node data into a dataframe
            music_attrs = music_info_to_music_node_attrs(music_info=music_info)
            music_df = music_attrs.to_df()
            list_music_df.append(music_df)

            list_video_hashtag_df: List[  # type: ignore[no-any-unimported]
                DataFrame
            ] = []
            for hashtag_info in list_hashtag_info:
                # Parse hashtag node data into a dictionary
                hashtag_attrs = hashtag_info_to_hashtag_node_attrs(
                    hashtag_info=hashtag_info
                )
                hashtag_df = hashtag_attrs.to_df()

                list_video_hashtag_df.append(hashtag_df)
            list_hashtag_df.extend(list_video_hashtag_df)

            #
            # Edge Parsing
            #

            # Identify subsections of response
            video_id = video_info["id"]
            author_id = author_info["id"]
            music_id = music_info["id"]

            # Parse author-to-video edge data into a dataframe
            author_to_video_attrs = id_to_author_to_video_edge_attrs(
                author_id=author_id, video_id=video_id
            )
            author_to_video_df = author_to_video_attrs.to_df()
            list_author_to_video_df.append(author_to_video_df)

            # Parse music-to-video edge data into a dataframe
            music_to_video_attrs = id_to_music_to_video_edge_attrs(
                music_id=music_id, video_id=video_id
            )
            music_to_video_df = music_to_video_attrs.to_df()
            list_music_to_video_df.append(music_to_video_df)

            for hashtag_df in list_video_hashtag_df:
                hashtag_id = hashtag_df.squeeze()[
                    (NodeType.hashtag.value, NodeAttrKey.id.value)
                ]

                # Parse video-to-hashtag edge data into a dataframe
                video_to_hashtag_attrs = id_to_video_to_hashtag_edge_attrs(
                    video_id=video_id, hashtag_id=hashtag_id
                )
                video_to_hashtag_df = video_to_hashtag_attrs.to_df()
                list_video_to_hashtag_df.append(video_to_hashtag_df)

            n_video_response += 1

        logger.info(
            f"Completed parsing API responses of {n_video_response} videos "
            f"into {len(list_video_df)} video dataframes, "
            f"{len(list_author_df)} author dataframes, "
            f"{len(list_music_df)} music dataframes, "
            f"{len(list_hashtag_df)} hashtag dataframes, "
            f"{len(list_author_to_video_df)} author-to-video dataframes, "
            f"{len(list_music_to_video_df)} music-to-video dataframes, "
            f"and {len(list_video_to_hashtag_df)} video-to-hashtag dataframes"
        )

    # Aggregate dataframes under each node and edge type
    agg_video_df = pd.concat(list_video_df, ignore_index=True)
    agg_author_df = pd.concat(list_author_df, ignore_index=True)
    agg_music_df = pd.concat(list_music_df, ignore_index=True)
    agg_hashtag_df = pd.concat(list_hashtag_df, ignore_index=True)

    agg_author_to_video_df = pd.concat(list_author_to_video_df, ignore_index=True)
    agg_music_to_video_df = pd.concat(list_music_to_video_df, ignore_index=True)
    agg_video_to_hashtag_df = pd.concat(list_video_to_hashtag_df, ignore_index=True)

    # Report null distribution
    logger.info(
        "Here is a record of numbers of null values per column in video dataframe:\n"
        f"{video_df.isnull().sum(axis=0)}"
    )

    logger.info(
        f"Complete parsing TikTok videos of hastag '{api_hashtag}' "
        "into dataframes. Statistics over video dataframe is as followed:\n"
        f"{video_df.describe()}"
    )
    logger.info(f"Here's a sample of video dataframe content:\n{video_df.head()}")

    # Assemble result object
    tiktok_database = TikTokDataBase(
        video_df=agg_video_df,
        author_df=agg_author_df,
        music_df=agg_music_df,
        hashtag_df=agg_hashtag_df,
        author_to_video_df=agg_author_to_video_df,
        music_to_video_df=agg_music_to_video_df,
        video_to_hashtag_df=agg_video_to_hashtag_df,
    )

    return tiktok_database


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
