""" Tests schema nuggets """
import json
import os

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from cupyopt.nuggets import schema

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
    """Tests schema nugget: avro_shema w tmpdir"""
    # create a sample file from the sample avro schema dict
    avsc_filepath = f"{tmpdir}/example.avsc"
    with open(avsc_filepath, "w", encoding="utf-8") as avro_file:
        avro_file.write(json.dumps(AVSC_DICT))

    assert isinstance(schema.avro_schema(avsc=AVSC_DICT), dict)
    assert isinstance(schema.avro_schema(avsc=avsc_filepath), dict)


def test_infer_df_avro_schema():
    """Tests schema nugget: infer_df_avro_schema"""
    # infer the avro schema from dataframe
    avsc = schema.infer_df_avro_schema(dataframe=DF, name="dataframe", namespace="test")

    assert avsc["type"] == "record"
    assert avsc["name"] == "test.dataframe"
    assert len(avsc["fields"]) == 3
    assert [field["name"] for field in avsc["fields"]] == ["C", "D", "E"]


def test_avro_schema_to_file(tmpdir):
    """Tests schema nugget: avro_schema_to_file"""
    # infer the avro schema from dataframe
    avsc = schema.infer_df_avro_schema(dataframe=DF, name="dataframe", namespace="test")

    # avsc to file
    filepath = schema.avro_schema_to_file(
        avsc=avsc, filename=avsc["name"], filedir=tmpdir
    )

    assert os.path.isfile(filepath)
    assert filepath.lower().endswith("avsc")
    assert isinstance(schema.avro_schema(avsc=filepath), dict)


def test_arrow_schema(tmpdir):
    """Tests schema nugget: arrow schema functions"""
    # infer the avro schema from dataframe
    arsc = schema.infer_df_arrow_schema(dataframe=DF)

    # arsc to file
    filepath = schema.arrow_schema_to_parquet(
        arsc=arsc, filename="sample", filedir=tmpdir
    )

    assert os.path.isfile(filepath)
    assert filepath.lower().endswith(".schema.parquet")
    assert isinstance(schema.arrow_schema_from_parquet(source=filepath), pa.lib.Schema)
    assert isinstance(
        schema.arrow_schema_from_parquet(source=pq.read_table(source=filepath)),
        pa.lib.Schema,
    )
