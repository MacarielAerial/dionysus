from __future__ import annotations

from dataclasses import dataclass, fields, replace
from logging import Logger
from pathlib import Path
from typing import Dict

import pandas as pd
from pandas import DataFrame

from ..nodes.pandas_operation import all_column_indices_to_categorical


@dataclass
class TikTokDataBase:  # type: ignore[no-any-unimported]
    # Node
    video_df: DataFrame  # type: ignore[no-any-unimported]
    author_df: DataFrame  # type: ignore[no-any-unimported]
    music_df: DataFrame  # type: ignore[no-any-unimported]
    hashtag_df: DataFrame  # type: ignore[no-any-unimported]

    # Edge
    author_to_video_df: DataFrame  # type: ignore[no-any-unimported]
    music_to_video_df: DataFrame  # type: ignore[no-any-unimported]
    video_to_hashtag_df: DataFrame  # type: ignore[no-any-unimported]

    @staticmethod
    def all_df_column_indices_to_categorical(
        tiktok_database: TikTokDataBase, logger: Logger
    ) -> None:
        for field in fields(tiktok_database):
            df = getattr(tiktok_database, field.name)
            df = all_column_indices_to_categorical(df=df, logger=logger)
            dict_replace: Dict[str, DataFrame] = {  # type: ignore[no-any-unimported]
                field.name: df
            }
            tiktok_database = replace(tiktok_database, **dict_replace)

            logger.debug(
                f"Converted column indces of {field.name} field to categorical"
            )


class TikTokDataBaseDataSet:
    def __init__(self, filepath: Path, logger: Logger) -> None:
        self.filepath = filepath
        self.logger = logger

    def save(self, tiktok_database: TikTokDataBase) -> None:
        self._save(
            filepath=self.filepath, tiktok_database=tiktok_database, logger=self.logger
        )

    @staticmethod
    def _save(filepath: Path, tiktok_database: TikTokDataBase, logger: Logger) -> None:
        with pd.HDFStore(filepath) as hdf5_store:
            hdf5_store.put("/nodes/video_df", tiktok_database.video_df, format="t")
            hdf5_store.put("/nodes/author_df", tiktok_database.author_df, format="t")
            hdf5_store.put("/nodes/music_df", tiktok_database.music_df, format="t")
            hdf5_store.put("/nodes/hashtag_df", tiktok_database.hashtag_df, format="t")

            hdf5_store.put(
                "/edges/author_to_video_df",
                tiktok_database.author_to_video_df,
                format="t",
            )
            hdf5_store.put(
                "/edges/music_to_video_df",
                tiktok_database.music_to_video_df,
                format="t",
            )
            hdf5_store.put(
                "/edges/video_to_hashtag_df",
                tiktok_database.video_to_hashtag_df,
                format="t",
            )

            hdf5_store.close()

            logger.info(f"Saved a {type(tiktok_database)} type object to {filepath}")

    def load(self) -> TikTokDataBase:
        return self._load(filepath=self.filepath, logger=self.logger)

    @staticmethod
    def _load(filepath: Path, logger: Logger) -> TikTokDataBase:
        with pd.HDFStore(filepath) as hdf5_store:
            logger.debug(f"HDF5 store file structure:\n{hdf5_store._handle}")

            video_df = hdf5_store["/nodes/video_df"]
            author_df = hdf5_store["/nodes/author_df"]
            music_df = hdf5_store["/nodes/music_df"]
            hashtag_df = hdf5_store["/nodes/hashtag_df"]

            author_to_video_df = hdf5_store["/edges/author_to_video_df"]
            music_to_video_df = hdf5_store["/edges/music_to_video_df"]
            video_to_hashtag_df = hdf5_store["/edges/video_to_hashtag_df"]

            tiktok_database = TikTokDataBase(
                video_df=video_df,
                author_df=author_df,
                music_df=music_df,
                hashtag_df=hashtag_df,
                author_to_video_df=author_to_video_df,
                music_to_video_df=music_to_video_df,
                video_to_hashtag_df=video_to_hashtag_df,
            )

            TikTokDataBase.all_df_column_indices_to_categorical(
                tiktok_database=tiktok_database, logger=logger
            )

            logger.info(
                f"Loaded a {type(tiktok_database)} type object from {filepath} "
            )

            return tiktok_database
