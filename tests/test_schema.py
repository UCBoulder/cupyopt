import os
import sys

import pytest

sys.path.append(os.getcwd())

import pandas as pd
import src.cupyopt.nuggets.schema as schema

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


def test_infer_df_avro_schema(tmpdir):
    # create a sample file from the sample avro schema dict
    avsc_filepath = "{}/example.avsc".format(tmpdir)
    with open(avsc_filepath, "w") as f:
        f.write(json.dumps(avsc_dict))

    assert isinstance(schema.avro_schema(avsc=avsc_dict), dict)
    assert isinstance(schema.avro_schema(avsc=avsc_filepath), dict)


def test_infer_df_avro_schema():
    # infer the avro schema from dataframe
    avsc = schema.infer_df_avro_schema(df=df, name="dataframe", namespace="test")

    assert avsc["type"] == "record"
    assert avsc["name"] == "test.dataframe"
    assert len(avsc["fields"]) == 3
    assert [field["name"] for field in avsc["fields"]] == ["A", "B", "C"]


def test_avro_schema_to_file(tmpdir):
    # infer the avro schema from dataframe
    avsc = schema.infer_df_avro_schema(df=df, name="dataframe", namespace="test")

    # avsc to file
    filepath = schema.avro_schema_to_file(
        avsc=avsc, filename=avsc["name"], filedir=tmpdir
    )

    assert os.path.isfile(filepath)
    assert filepath.lower().endswith("avsc")
    assert isinstance(schema.avro_schema(avsc=filepath), dict)
