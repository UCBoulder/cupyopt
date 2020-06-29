import logging

import pandas as pd
import pandera as pa
from box import Box


def PaSchema(yaml_schema) -> pa.DataFrameSchema:
    logging.info("Creating Pandera DataFrameSchema from Yaml-based schema.")
    return pa.io.from_yaml(yaml_schema)


def PaValidate(df: pd.DataFrame, schema: pa.DataFrameSchema) -> pd.DataFrame:
    logging.info("Validating Pandas DataFrame using Pandera DataFrameSchema.")
    return schema.validate(df)