""" Tests schema nuggets """
import os
import sys

sys.path.append(os.getcwd())

import json
import pandas as pd
import src.cupyopt.nuggets.schema as schema

# create a test dataframe
DF = pd.DataFrame(
    {"C": [4, 5, 6], "D": ["Lemurs", "Leopards", "Lichen"], "E": [7.7, 8.8, 9.9]}
)

# create a test avro schema dict
AVSC_DICT = {
    "type": "record",
    "name": "schema_test",
    "fields": [
        {"name": "C", "type": ["null", "long"]},
        {"name": "D", "type": ["null", "string"]},
        {"name": "E", "type": ["null", "double"]},
    ],
}


def test_avro_schema(tmpdir):
    """ Tests schema nugget: avro_shema w tmpdir"""
    # create a sample file from the sample avro schema dict
    avsc_filepath = "{}/example.avsc".format(tmpdir)
    with open(avsc_filepath, "w") as avro_file:
        avro_file.write(json.dumps(AVSC_DICT))

    assert isinstance(schema.avro_schema(avsc=AVSC_DICT), dict)
    assert isinstance(schema.avro_schema(avsc=avsc_filepath), dict)


def test_infer_df_avro_schema():
    """ Tests schema nugget: infer_df_avro_schema """
    # infer the avro schema from dataframe
    avsc = schema.infer_df_avro_schema(dataframe=DF, name="dataframe", namespace="test")

    assert avsc["type"] == "record"
    assert avsc["name"] == "test.dataframe"
    assert len(avsc["fields"]) == 3
    assert [field["name"] for field in avsc["fields"]] == ["C", "D", "E"]


def test_avro_schema_to_file(tmpdir):
    """ Tests schema nugget: avro_schema_to_file """
    # infer the avro schema from dataframe
    avsc = schema.infer_df_avro_schema(dataframe=DF, name="dataframe", namespace="test")

    # avsc to file
    filepath = schema.avro_schema_to_file(
        avsc=avsc, filename=avsc["name"], filedir=tmpdir
    )

    assert os.path.isfile(filepath)
    assert filepath.lower().endswith("avsc")
    assert isinstance(schema.avro_schema(avsc=filepath), dict)
