import pandas as pd
import pysftp
import datetime
import logging
import prefect
import os
import tempfile

from typing import Any, Optional
from prefect import Task
from box import Box
import pandera as pa

class PaSchemaFromFile(Task):
    """
    Creates Pd Dataframe dictionaries and related Pa DataFrameSchema
    
    Return a Box containing named datadictionary dataframes and pandera dataframeschema
    """

    def __init__(self, **kwargs: Any):
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

    def run(self, filepaths: Box, **format_kwargs: Any) -> str:
        with prefect.context(**format_kwargs) as data:

            return_schema = Box()
            for name, filepath in filepaths.items():
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

    def __init__(self, **kwargs: Any):
        super().__init__(**kwargs)

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

            for name, df in record.dataframe.items():
                dataframe[name] = schema[name].validate(
                    record.dataframe[name],
                    head=head,
                    tail=tail,
                    sample=sample,
                    random_state=random_state,
                )

            return dataframe