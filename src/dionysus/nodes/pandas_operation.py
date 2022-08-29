from logging import Logger

import pandas as pd
from pandas import DataFrame


def all_column_indices_to_categorical(  # type: ignore[no-any-unimported]
    df: DataFrame, logger: Logger
) -> DataFrame:
    # Change datatype of the multiindex
    logger.debug(f"DataFrame column data types before mutation:\n{df.columns.dtypes}")

    for i in range(len(df.columns.levels)):
        df.columns = df.columns.set_levels(
            df.columns.levels[i].astype(pd.CategoricalDtype()), level=i
        )

        logger.debug(
            f"DataFrame column data types after mutation:\n{df.columns.dtypes}"
        )

    return df
