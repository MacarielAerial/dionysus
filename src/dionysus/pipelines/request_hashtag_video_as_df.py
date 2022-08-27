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
from ..nodes.api_response_parsing import (
    author_info_stats_to_author_node_attrs,
    hashtag_info_to_hashtag_node_attrs,
    music_info_to_music_node_attrs,
    video_info_to_video_node_attrs,
)
from ..nodes.nx_g_schema import NodeType
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

    # Load cookie
    msToken = os.environ.get("msToken")

    # Roughly mark the datetime of data collection
    now = datetime.now()

    logger.info(f"Datetime of request is marked as {now}")

    # Initiate a random number generator for request wait time
    randint_gen = return_gen_randint(start=1, end=10, n_iter=n_video + 30)

    logger.info(f"Querying hashtag '{hashtag}'...")

    with TikTokApi(msToken=msToken, logger=logger) as api:
        api_hashtag = api.hashtag(name=hashtag)
        hashtag_info = api_hashtag.info(request_delay=next(randint_gen))

        logger.debug(
            f"Hastag '{hashtag}' has the following info:\n" f"{pformat(hashtag_info)}"
        )

        # Parse hashtag node data into a dictionary
        hashtag_attrs = hashtag_info_to_hashtag_node_attrs(hashtag_info=hashtag_info)
        dict_hashtag_info = hashtag_attrs.to_multi_index_dict_native(
            ntype=NodeType.hashtag
        )

        video_gen = api_hashtag.videos(count=n_video, request_delay=next(randint_gen))
        n_video_response = 0
        for video in video_gen:
            logger.info(f"Iterating over {n_video_response}th video...")

            video_info: Dict[str, Any] = video.info(request_delay=next(randint_gen))
            author_info = video_info["author"]
            author_stats = video_info["authorStats"]
            music_info = video_info["music"]

            # Parse video node data into a dictionary
            video_attrs = video_info_to_video_node_attrs(video_info=video_info)
            dict_video_info = video_attrs.to_multi_index_dict_native(
                ntype=NodeType.video
            )

            # Parse author node data into a dictionary
            author_attrs = author_info_stats_to_author_node_attrs(
                author_info=author_info, author_stats=author_stats
            )
            dict_author_info = author_attrs.to_multi_index_dict_native(
                ntype=NodeType.author
            )

            # Parse music node data into a dictionary
            music_attrs = music_info_to_music_node_attrs(music_info=music_info)
            dict_music_info = music_attrs.to_multi_index_dict_native(
                ntype=NodeType.music
            )

            # Parse data of nodes of all types into a pandas series
            dict_multi_ntype_info = (
                dict_video_info | dict_author_info | dict_music_info | dict_hashtag_info
            )
            s = Series(
                dict_multi_ntype_info.values(),
                index=pd.MultiIndex.from_tuples(
                    dict_multi_ntype_info.keys(), names=["ntype", "nfeat"]
                ),
                name=n_video_response,
            )

            n_video_response += 1

            # Persist parsed series
            list_single_col_df.append(s.to_frame().T)

        logger.info(
            f"Completed parsing API responses of {n_video_response} videos "
            f"into {len(list_single_col_df)} series"
        )

    # Compile list of series into a dataframe
    df = pd.concat(list_single_col_df)
    df.attrs.update({"datetime_request": now.isoformat()})

    # Report null distribution
    logger.info(
        "Here is a record of numbers of null values per column:\n"
        f"{df.isnull().sum(axis=0)}"
    )

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
