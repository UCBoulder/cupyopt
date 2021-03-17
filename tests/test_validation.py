import os
import sys

import pytest

sys.path.append(os.getcwd())

import pandas as pd
import src.cupyopt.validation as validation

# create a test dataframe
df = pd.DataFrame(
    {"A": [1, 2, 3], "B": [4.4, 5.5, 6.6], "C": ["Lions", "Tigers", "Pandas"]}
)

# create a test avro schema dict
avsc_dict = {
    "type": "record",
    "name": "test",
    "fields": [
        {"name": "A", "type": ["null", "long"]},
        {"name": "B", "type": ["null", "double"]},
        {"name": "C", "type": ["null", "string"]},
    ],
}


def test_df_avro_validate():
    assert validation.df_avro_validate(df=df, avsc=avsc_dict)
