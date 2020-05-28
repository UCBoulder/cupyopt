import pandas as pd
import pysftp
import datetime
import logging
import prefect
import os
import tempfile

from typing import Any, Optional
from prefect import Task
from prefect.utilities.tasks import defaults_from_attrs
from box import Box
import pandera as pa


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


class PdDatadictTranslate(Task):
    """
    Transforms dataframes to pre-specified formats found in custom Pandas Dataframe data-dictionary content.
    
    Returns a translated pd.Dataframe
    """

    def __init__(
        self,
        df: pd.DataFrame = None,
        datadict: pd.DataFrame = None,
        oradb_prep: bool = False,
        no_col_names: bool = False,
        **kwargs: Any
    ):
        self.df = df
        self.datadict = datadict
        self.oradb_prep = oradb_prep
        self.no_col_names = no_col_names
        super().__init__(**kwargs)

    def set_field_numeric_column_names(self, data: pd.DataFrame) -> pd.DataFrame:
        # rename columns which may be "blank" from source data to make column names strings and readable

        data.columns = ["field_" + str(x) for x in list(data.columns)]

        return data

    def prep_null_values_for_oradb(self, data: pd.DataFrame) -> pd.DataFrame:
        # set NA values to 0 or space character, contingent on type found within the datatype

        for col in list(data.columns):
            # NB The meaning of biufc: b bool, i int (signed), u unsigned int, f float, c complex.
            # See https://docs.scipy.org/doc/numpy/reference/generated/numpy.dtype.kind.html#numpy.dtype.kind
            if data[col].dtype.kind in "biufc":
                data[col] = data[col].fillna(0)
            else:
                data[col] = data[col].fillna(" ")

        return data

    @defaults_from_attrs(
        "df", "datadict", "oradb_prep", "no_col_names",
    )
    def run(
        self,
        df: pd.DataFrame = None,
        datadict: pd.DataFrame = None,
        oradb_prep: bool = False,
        no_col_names: bool = False,
        **format_kwargs: Any
    ) -> pd.DataFrame:

        with prefect.context(**format_kwargs) as data:

            if datadict.empty:
                raise Exception(
                    "Datadictionary is empty! Provide a non-empty datadict in order to proceed."
                )

            self.logger.info("Translating dataframe using datadictionary values.")

            try:
                assert len(set(df.columns)) == len(df.columns)
            except AssertionError as e:
                raise AssertionError(
                    "There may be duplicate column names in the DataFrame provided. Please ensure column names are unique."
                )

            # if no col names true, we need to update the colnames first
            if no_col_names:
                df = self.set_field_numeric_column_names(df)

            # Remap column names from related_name -to-> name
            df = df.rename(
                columns=datadict[datadict["related_name"].notnull()]
                .set_index("related_name")["name"]
                .to_dict()
            )

            # Convert to expected pandas dtypes per column
            df = df.astype(
                datadict[datadict["name"].isin(df.columns)]
                .set_index("name")["pandas_dtype"]
                .to_dict()
            )

            # Ensure we set cols of object type to strings explicitly from pandas
            for col in df.columns:
                if str(df[col].dtype) == "object":
                    df[col] = df[col].astype("str")

            # If oradb_prep true, we need to prepare values for oracle db
            if oradb_prep:
                df = self.prep_null_values_for_oradb(df)

            # Reset the cols of the dataframe to drop unrelated cols we no longer need
            # (dataframe may have had more than dictionary)
            df = df[datadict[datadict["name"].isin(df.columns)]["name"].values.tolist()]

            return df
