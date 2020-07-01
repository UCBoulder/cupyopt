import logging

import pandas as pd
import pandera as pa
from box import Box


def pa_schema(yaml_schema) -> pa.DataFrameSchema:
    logging.info("Creating Pandera DataFrameSchema from Yaml-based schema.")
    return pa.io.from_yaml(yaml_schema)


def pa_validate(df: pd.DataFrame, schema: pa.DataFrameSchema) -> pd.DataFrame:
    logging.info("Validating Pandas DataFrame using Pandera DataFrameSchema.")
    return schema.validate(df)