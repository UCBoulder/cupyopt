""" Validation functions """
import logging

import fastavro as avro
import pandas as pd
import pyarrow as pa

logger = logging.getLogger(__name__)  # pylint: disable=C0103


def df_arrow_validate(dataframe: pd.DataFrame, arsc: pa.lib.Schema) -> bool:
    """Validate Pandas dataframe against arrow schema"""
    logger.info("Validating dataframe against arrow schema")
    return arsc.equals(pa.Schema.from_pandas(dataframe))


def df_avro_validate(dataframe: pd.DataFrame, avsc: dict) -> bool:
    """Validate Pandas dataframe against avro schema dict"""
    logger.info("Validating dataframe against avro schema dict.")
    return avro.validation.validate_many(
        records=dataframe.replace(pd.NA, "").to_dict(orient="records"), schema=avsc
    )
