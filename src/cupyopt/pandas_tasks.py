import datetime
import logging
import os
import tempfile
from typing import Any, Optional

import pandas as pd
import prefect
import pysftp
from box import Box
from prefect import Task
from prefect.utilities.tasks import defaults_from_attrs


class PdDataFrameFromCSV(Task):
    """
    Read CSV file into a Pandas DataFrame

    Return a Pandas DataFrame
    """

    def __init__(self, filepath: str = None, **kwargs: Any):
        self.filepath = filepath
        super().__init__(**kwargs)

    @defaults_from_attrs("filepath")
    def run(self, filepath: str = None, **format_kwargs: Any) -> pd.DataFrame:
        with prefect.context(**format_kwargs) as data:

            self.logger.info("Reading CSV file {} into dataframe.".format(filepath))
            df = pd.read_csv(filepath)

            return df


class PdDataFrameFromParquet(Task):
    """
    Read Parquet file into a Pandas DataFrame

    Return a Pandas DataFrame
    """

    def __init__(self, filepath: str = None, **kwargs: Any):
        self.filepath = filepath
        super().__init__(**kwargs)

    @defaults_from_attrs("filepath")
    def run(self, filepath: str = None, **format_kwargs: Any) -> pd.DataFrame:
        with prefect.context(**format_kwargs) as data:

            self.logger.info("Reading Parquet file {} into dataframe.".format(filepath))
            df = pd.read_parquet(filepath)

            return df


class PdDataFrameToCSV(Task):
    """
    Exports dataframes using temporary dicectory, DataFrame name, name suffix, and config

    Return a filepaths for the exported Dataframe
    """

    def __init__(
        self,
        df: pd.DataFrame = None,
        df_name: str = None,
        temp_name: bool = False,
        df_name_prefix: str = "",
        df_name_suffix: str = "",
        dir_name: str = "",
        config_box: Box = None,
        index=True,
        header=True,
        **kwargs: Any
    ):
        self.df = df
        self.df_name = df_name
        self.temp_name = temp_name
        self.df_name_prefix = df_name_prefix
        self.df_name_suffix = df_name_suffix
        self.dir_name = dir_name
        self.config_box = config_box
        self.index = index
        self.header = header
        super().__init__(**kwargs)

    @defaults_from_attrs(
        "df",
        "df_name",
        "temp_name",
        "df_name_prefix",
        "df_name_suffix",
        "dir_name",
        "config_box",
        "index",
        "header",
    )
    def run(
        self,
        df: pd.DataFrame = None,
        df_name: str = None,
        temp_name: bool = False,
        df_name_prefix: str = "",
        df_name_suffix: str = "",
        dir_name: str = "",
        config_box: Box = None,
        index=True,
        header=True,
        **format_kwargs: Any
    ) -> str:
        with prefect.context(**format_kwargs) as data:

            if temp_name and dir_name != "":
                filepath = mkstemp(
                    suffix=df_name_suffix, prefix=df_name_prefix, dir=dir_name
                )[1]

            elif config_box and dir_name == "":
                filepath = os.path.join(
                    config_box.extracttempdir,
                    "{}{}{}.csv".format(df_name_prefix, df_name, df_name_suffix),
                )
            else:
                filename = "{}{}{}.csv".format(df_name_prefix, df_name, df_name_suffix)
                filepath = os.path.join(dir_name, filename)

            self.logger.info("Creating CSV file {} from dataframe.".format(filepath))
            df.to_csv(filepath, index=index, header=header)

            return filepath


class PdDataFrameToParquet(Task):
    """
    Exports dataframe to parquet using various options

    Return a filepaths for the exported Dataframe
    """

    def __init__(
        self,
        df: pd.DataFrame = None,
        df_name: str = None,
        temp_name: bool = False,
        df_name_prefix: str = "",
        df_name_suffix: str = "",
        dir_name: str = "",
        config_box: Box = None,
        index=True,
        **kwargs: Any
    ):
        self.df = df
        self.df_name = df_name
        self.temp_name = temp_name
        self.df_name_prefix = df_name_prefix
        self.df_name_suffix = df_name_suffix
        self.dir_name = dir_name
        self.config_box = config_box
        self.index = index
        super().__init__(**kwargs)

    @defaults_from_attrs(
        "df",
        "df_name",
        "temp_name",
        "df_name_prefix",
        "df_name_suffix",
        "dir_name",
        "config_box",
        "index",
    )
    def run(
        self,
        df: pd.DataFrame = None,
        df_name: str = None,
        temp_name: bool = False,
        df_name_prefix: str = "",
        df_name_suffix: str = "",
        dir_name: str = "",
        config_box: Box = None,
        index=True,
        **format_kwargs: Any
    ) -> str:
        with prefect.context(**format_kwargs) as data:

            if temp_name and dir_name != "":
                filepath = mkstemp(
                    suffix=df_name_suffix, prefix=df_name_prefix, dir=dir_name
                )[1]

            elif config_box and dir_name == "":
                filepath = os.path.join(
                    config_box.extracttempdir,
                    "{}{}{}.parquet".format(df_name_prefix, df_name, df_name_suffix),
                )
            else:
                filename = "{}{}{}.parquet".format(
                    df_name_prefix, df_name, df_name_suffix
                )
                filepath = os.path.join(dir_name, filename)

            self.logger.info(
                "Creating Parquet file {} from dataframe.".format(filepath)
            )

            df.to_parquet(path=filepath, index=index)

            return filepath


class PdColRenameAndFilter(Task):
    """
    Rename and filter Pandas Dataframe columns using python dictionary.

    Column names provided in coldict follow the same format as expected by
    pd.DataFrame.rename(columns=dict). For example: {"current":"new", "current2":"new2"}

    Columns in returned dataframe are filtered by those provided to be renamed.

    Returns a modified pd.Dataframe copy
    """

    def __init__(self, df: pd.DataFrame = None, coldict: dict = None, **kwargs: Any):
        self.df = df
        self.coldict = coldict
        super().__init__(**kwargs)

    @defaults_from_attrs("df", "coldict")
    def run(
        self, df: pd.DataFrame, coldict: dict, **format_kwargs: Any
    ) -> pd.DataFrame:

        with prefect.context(**format_kwargs) as data:

            self.logger.info(
                "Renaming and filtering dataframe columns using coldict key:values."
            )

            # Remap column names
            df = df.rename(columns=coldict)

            # Filter columns based on the new names
            df = df[[val for key, val in coldict.items()]].copy()

            return df
