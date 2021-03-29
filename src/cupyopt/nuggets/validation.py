import logging

import fastavro as avro
import pandas as pd
from box import Box


def df_avro_validate(df: pd.DataFrame, avsc: dict) -> bool:
    return avro.validation.validate_many(
        records=df.replace(pd.NA, "").to_dict(orient="records"), schema=avsc
    )
