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


class PaSchemaFromFile(Task):
    """
    Creates Pd Dataframe dictionaries and related Pa DataFrameSchema
    
    Return a Box containing named datadictionary dataframes and pandera dataframeschema
    """

    def __init__(self, filepaths: Box, **kwargs: Any):
        self.filepaths = filepaths
        super().__init__(**kwargs)

    def pandera_schema_from_dataframe(self, pa_df: pd.DataFrame) -> pa.DataFrameSchema:
        pa_cols = {}

        for row in pa_df.to_dict(orient="records"):
            pa_cols[row["name"]] = pa.Column(
                name=row["name"],
                pandas_dtype=row["pandas_dtype"],
                nullable=row["nullable"],
                allow_duplicates=row["allow_duplicates"],
                coerce=row["coerce"],
                required=row["required"],
                regex=row["regex"],
                checks=None,
            )

        return pa.DataFrameSchema(pa_cols)

    @defaults_from_attrs("filepaths")
    def run(self, filepaths: Box, **format_kwargs: Any) -> Box:

        return_schema = Box()
        for name, filepath in filepaths.items():
            self.logger.info(
                "Creating Pandera schema '{}' using datadictionary based on file {}".format(
                    name, filepath
                )
            )
            return_schema[name] = {"datadict": pd.read_csv(filepath)}
            return_schema[name]["schema"] = self.pandera_schema_from_dataframe(
                return_schema[name].datadict
            )

        return return_schema


class PaValidate(Task):
    """
    Validates Pandas Dataframes with related Pandera DataFrameSchema. 
    
    Note: assumes Box configuration with names that match between Dataframe and DataFrameSchema
    
    Return a Box containing named pandas DataFrames
    """

    def __init__(
        self,
        dataframe: Box,
        schema: Box,
        head: Optional[int] = None,
        tail: Optional[int] = None,
        sample: Optional[int] = None,
        random_state: Optional[int] = None,
        **kwargs: Any
    ):
        self.dataframe = dataframe
        self.schema = schema
        self.head = head
        self.tail = tail
        self.sample = sample
        self.random_state = random_state
        super().__init__(**kwargs)

    @defaults_from_attrs(
        "dataframe", "schema", "head", "tail", "sample", "random_state",
    )
    def run(
        self,
        dataframe: Box,
        schema: Box,
        head: Optional[int] = None,
        tail: Optional[int] = None,
        sample: Optional[int] = None,
        random_state: Optional[int] = None,
        **format_kwargs: Any
    ) -> Box:

        with prefect.context(**format_kwargs) as data:

            for name, df in dataframe.dataframe.items():
                self.logger.info(
                    "Validating dataframe '{}' using Pandera schema".format(name)
                )
                dataframe[name] = schema[name].schema.validate(
                    dataframe[name],
                    head=head,
                    tail=tail,
                    sample=sample,
                    random_state=random_state,
                )

            return dataframe


class PaDatadictTranslate(Task):
    """
    Transforms dataframes to pre-specified formats found in custom Pandera data-dictionary content.
    
    Note: expects schema to be of format returned from PaSchemaFromFile task.
    Note: assumes Box configuration with names that match between Dataframe and DataFrameSchema
    
    Returns a Box containing named pandas DataFrames
    """

    def __init__(
        self,
        dataframe: Box,
        schema: Box,
        oradb_prep: bool = False,
        no_col_names: bool = False,
        **kwargs: Any
    ):
        self.dataframe = dataframe
        self.schema = schema
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
        "dataframe", "schema", "oradb_prep", "no_col_names",
    )
    def run(
        self,
        dataframe: Box,
        schema: Box,
        oradb_prep: bool = False,
        no_col_names: bool = False,
        **format_kwargs: Any
    ) -> Box:

        with prefect.context(**format_kwargs) as data:

            # loop through each dataframe item in the box provided
            for name, df in dataframe.dataframe.items():

                self.logger.info(
                    "Translating dataframe '{}' using datadictionary values.".format(
                        name
                    )
                )

                # if no col names true, we need to update the colnames first
                if no_col_names:
                    df = self.set_field_numeric_column_names(df)

                # Remap column names from related_name -to-> name
                df = df.rename(
                    columns=schema[name]
                    .datadict[schema[name].datadict["related_name"].notnull()]
                    .set_index("related_name")["name"]
                    .to_dict()
                )

                # Convert to expected pandas dtypes per column
                df = df.astype(
                    schema[name].datadict.set_index("name")["pandas_dtype"].to_dict()
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
                df = df[schema[name].datadict["name"].values.tolist()]

                # Make sure we replace the existing boxed dataframe with our new one
                dataframe[name] = df

            return dataframe