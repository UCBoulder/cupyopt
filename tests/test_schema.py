""" Tests schema nuggets """
import json
import os

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from cupyopt.schema_tasks import (
    ArrowSchemaFromParquet,
    AvroSchema,
    AvroSchemaToFile,
    DFInferArrowSchema,
    DFInferAvroSchema,
    ArrowSchemaToParquet,
    DFValidateSchemaAvro,
    DFValidateSchemaArrow,
)

# create a test dataframe
sample_df = pd.DataFrame(
    {"C": [4, 5, 6], "D": ["Lemurs", "Leopards", "Lichen"], "E": [7.7, 8.8, 9.9]}
)

# create a test avro schema dict
sample_avsc_dict = {
    "type": "record",
    "name": "schema_test",
    "fields": [
        {"name": "C", "type": ["null", "long"]},
        {"name": "D", "type": ["null", "string"]},
        {"name": "E", "type": ["null", "double"]},
    ],
}


def test_avro_schema(tmpdir):
    """Tests avro schema task with tmpdir"""
    # create a sample file from the sample avro schema dict
    avsc_filepath = f"{tmpdir}/example.avsc"
    with open(avsc_filepath, "w", encoding="utf-8") as avro_file:
        avro_file.write(json.dumps(sample_avsc_dict))

    assert isinstance(AvroSchema().run(avsc=sample_avsc_dict), dict)
    assert isinstance(AvroSchema().run(avsc=avsc_filepath), dict)


def test_df_infer_avro_schema():
    """Tests df infer avro schema task"""
    # infer the avro schema from dataframe
    avsc = DFInferAvroSchema().run(
        dataframe=sample_df, schemaname="dataframe", namespace="test"
    )

    assert avsc["type"] == "record"
    assert avsc["name"] == "test.dataframe"
    assert len(avsc["fields"]) == 3
    assert [field["name"] for field in avsc["fields"]] == ["C", "D", "E"]


def test_avro_schema_to_file(tmpdir):
    """Tests avro schema to file task"""
    # infer the avro schema from dataframe
    avsc = DFInferAvroSchema().run(
        dataframe=sample_df, schemaname="dataframe", namespace="test"
    )

    # avsc to file
    filepath = AvroSchemaToFile().run(avsc=avsc, filename=avsc["name"], filedir=tmpdir)

    assert os.path.isfile(filepath)
    assert filepath.lower().endswith("avsc")
    assert isinstance(AvroSchema().run(avsc=filepath), dict)


def test_arrow_schema_to_file(tmpdir):
    """Tests arrow schema to parquet file task"""
    # infer the avro schema from dataframe
    arsc = DFInferArrowSchema().run(dataframe=sample_df)

    # arsc to file
    filepath = ArrowSchemaToParquet().run(arsc=arsc, filename="sample", filedir=tmpdir)

    assert os.path.isfile(filepath)
    assert filepath.lower().endswith(".schema.parquet")
    assert isinstance(ArrowSchemaFromParquet().run(source=filepath), pa.lib.Schema)
    assert isinstance(
        ArrowSchemaFromParquet().run(source=pq.read_table(source=filepath)),
        pa.lib.Schema,
    )


def test_df_avro_validate():
    """Tests avro validation"""
    # create a test dataframe
    sample_df2 = pd.DataFrame(
        {"A": [1, 2, 3], "B": [4.4, 5.5, 6.6], "C": ["Lions", "Tigers", "Pandas"]}
    )

    # create a test avro schema dict
    sample_sample_avsc_dict = {
        "type": "record",
        "name": "validation_test",
        "fields": [
            {"name": "A", "type": ["null", "long"]},
            {"name": "B", "type": ["null", "double"]},
            {"name": "C", "type": ["null", "string"]},
        ],
    }

    assert DFValidateSchemaAvro().run(
        dataframe=sample_df2, avsc=sample_sample_avsc_dict
    )


def test_df_arrow_validate():
    """Tests arrow validation"""
    arsc = DFInferArrowSchema().run(sample_df)

    assert DFValidateSchemaArrow().run(sample_df, arsc)
