""" Validation functions """
import logging

import fastavro as avro
import pandas as pd

logger = logging.getLogger(__name__)


def df_avro_validate(df: pd.DataFrame, avsc: dict) -> bool:
    """ Validate Pandas dataframe against avro schema dict """
    logger.info("Validating dataframe against avro schema dict.")
    return avro.validation.validate_many(
        records=df.replace(pd.NA, "").to_dict(orient="records"), schema=avsc
    )
