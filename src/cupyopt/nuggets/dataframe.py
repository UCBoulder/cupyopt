""" Dataframe functions """
import logging
import os
from tempfile import mkstemp

import pandas as pd
from box import Box

# pylint: disable=too-many-arguments

logger = logging.getLogger(__name__)  # pylint: disable=C0103


def pd_export(
    dataframe: pd.DataFrame,
    export_type: str,
    df_name: str,
    temp_name: bool = False,
    df_name_prefix: str = "",
    df_name_suffix: str = "",
    dir_name: str = ".",
    config_box: Box = None,
    index=True,
    header=True,
) -> str:
    """
    Exports dataframe to file formats using various options

    Return a filepaths for the exported Dataframe
    """

    if temp_name and dir_name != "":
        filepath = mkstemp(suffix=df_name_suffix, prefix=df_name_prefix, dir=dir_name)[
            1
        ]

    elif config_box and dir_name == "":
        filepath = os.path.join(
            config_box.extracttempdir,
            f"{df_name_prefix}{df_name}{df_name_suffix}.{export_type}",
        )
    else:
        filename = f"{df_name_prefix}{df_name}{df_name_suffix}.{export_type}"
        filepath = os.path.join(dir_name, filename)

    logger.info("Creating %s file %s from dataframe.", export_type, filepath)

    if export_type == "parquet":
        dataframe.to_parquet(path=filepath, index=index)
    elif export_type == "csv":
        dataframe.to_csv(filepath, index=index, header=header)

    return filepath


def pd_colupdate(dataframe: pd.DataFrame, coldict: dict) -> pd.DataFrame:
    """
    Rename and filter Pandas Dataframe columns using python dictionary.

    Column names provided in coldict follow the same format as expected by
    pd.DataFrame.rename(columns=dict). For example: {"current":"new", "current2":"new2"}

    Columns in returned dataframe are filtered by those provided to be renamed.

    Returns a modified pd.Dataframe copy
    """

    logger.info("Renaming and filtering dataframe columns using coldict key:values.")

    # Remap column names
    dataframe = dataframe.rename(columns=coldict)

    # Filter columns based on the new names
    dataframe = dataframe[[val for key, val in coldict.items()]].copy()

    return dataframe
