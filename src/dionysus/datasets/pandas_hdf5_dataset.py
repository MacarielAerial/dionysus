from logging import Logger
from pathlib import Path

import pandas as pd
from pandas import DataFrame


class PandasHDF5DataSet:
    def __init__(self, filepath: Path, logger: Logger) -> None:
        self.filepath = filepath
        self.logger = logger

    def save(self, df: DataFrame) -> None:  # type: ignore[no-any-unimported]
        self._save(filepath=self.filepath, df=df, logger=self.logger)

    @staticmethod
    def _save(  # type: ignore[no-any-unimported]
        filepath: Path, df: DataFrame, logger: Logger
    ) -> None:
        with pd.HDFStore(filepath) as hdf5_store:
            hdf5_store.put("df", df)
            hdf5_store.get_storer("df").attrs.metadata = df.attrs
            hdf5_store.close()

            logger.info(f"Saved a {type(df)} type object to {filepath}")

    def load(self) -> DataFrame:  # type: ignore[no-any-unimported]
        return self._load(filepath=self.filepath, logger=self.logger)

    @staticmethod
    def _load(  # type: ignore[no-any-unimported]
        filepath: Path, logger: Logger
    ) -> DataFrame:
        with pd.HDFStore(filepath) as hdf5_store:
            df = hdf5_store["df"]
            metadata = hdf5_store.get_storer("df").attrs.metadata

            df.attrs = metadata

            logger.info(
                f"Loaded a {type(df)} type object from {filepath} "
                f"and its metadata '{df.attrs}'"
            )

            # Change datatype of the multiindex
            logger.debug(
                f"DataFrame column data types before mutation:\n{df.columns.dtypes}"
            )

            for i in range(len(df.columns.levels)):
                df.columns = df.columns.set_levels(
                    df.columns.levels[i].astype(pd.CategoricalDtype()), level=i
                )

            logger.debug(
                f"DataFrame column data types after mutation:\n{df.columns.dtypes}"
            )

            return df
