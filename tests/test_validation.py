""" Tests validation nuggets """
import os
import sys

sys.path.append(os.getcwd())

import pandas as pd
import src.cupyopt.nuggets.validation as validation
import src.cupyopt.nuggets.schema as schema

# create a test dataframe
DF = pd.DataFrame(
    {"A": [1, 2, 3], "B": [4.4, 5.5, 6.6], "C": ["Lions", "Tigers", "Pandas"]}
)

# create a test avro schema dict
AVSC_DICT = {
    "type": "record",
    "name": "validation_test",
    "fields": [
        {"name": "A", "type": ["null", "long"]},
        {"name": "B", "type": ["null", "double"]},
        {"name": "C", "type": ["null", "string"]},
    ],
}


def test_df_avro_validate():
    """ Tests validation nugget : df_avro_validate """
    assert validation.df_avro_validate(dataframe=DF, avsc=AVSC_DICT)


def test_df_arrow_validate():
    """ Tests validation nugget : df_arrow_validate """
    arsc = schema.infer_df_arrow_schema(dataframe=DF)

    assert validation.df_arrow_validate(dataframe=DF, arsc=arsc)
