""" Schema functions """
import json
import logging
from typing import Any, Union

import fastavro as avro
import pandas as pd
import pandavro as pda
import pyarrow as pa
import pyarrow.parquet as pq
from prefect import Task
from prefect.utilities.tasks import defaults_from_attrs

# pylint: disable=arguments-differ, too-many-arguments


class DFInferArrowSchema(Task):
    """Infer arrow schema from pandas dataframe"""

    def __init__(self, **kwargs: Any):
        super().__init__(**kwargs)

    def run(self, dataframe: pd.DataFrame) -> pa.lib.Schema:

        logging.info("Inferring arrow schema from dataframe")
        # infer the schema using pyarrow
        arsc = pa.Schema.from_pandas(df=dataframe)

        return arsc


class ArrowSchemaToParquet(Task):
    """Export arrow schema to parquet file"""

    def __init__(
        self,
        arsc: pa.lib.Schema = None,
        filename: str = None,
        filedir: str = ".",
        **kwargs: Any,
    ):
        self.arsc = arsc
        self.filename = filename
        self.filedir = filedir
        super().__init__(**kwargs)

    @defaults_from_attrs("arsc", "filename", "filedir")
    def run(self, arsc: pa.lib.Schema, filename: str, filedir: str = ".") -> str:

        logging.info("Exporting arrow schema to parquet file")

        filepath = f"{filedir}/{filename}.schema.parquet"

        # write schema into empty table parquet file
        pq.write_table(table=arsc.empty_table(), where=filepath)

        return filepath


class ArrowSchemaFromParquet(Task):
    """Import arrow schema from parquet file"""

    def __init__(
        self,
        source: Union[pa.lib.Table, str] = None,
        **kwargs: Any,
    ):
        self.source = source
        super().__init__(**kwargs)

    @defaults_from_attrs("source")
    def run(self, source: Union[pa.lib.Table, str]) -> pa.lib.Schema:

        logging.info("Importing arrow schema from parquet")
        if isinstance(source, pa.lib.Table):
            schema = source.schema

        elif isinstance(source, str):
            schema = pq.read_table(source=source).schema

        return schema


class AvroSchema(Task):
    """Create avro schema from dictionary or filepath string"""

    def __init__(
        self,
        **kwargs: Any,
    ):
        super().__init__(**kwargs)

    def run(self, avsc: Union[dict, str]) -> dict:

        # if a dictionary type, parse from dict
        logging.info("Parsing avro schema")
        if isinstance(avsc, dict):
            avsc = avro.schema.parse_schema(avsc)

        # if a str type, load from file
        elif isinstance(avsc, str):
            avsc = avro.schema.load_schema(avsc)

        return avsc


class DFInferAvroSchema(Task):
    """Infer avro schema from pandas dataframe"""

    def __init__(
        self,
        **kwargs: Any,
    ):
        super().__init__(**kwargs)

    def run(
        self,
        dataframe: pd.DataFrame,
        schemaname: str = None,
        namespace: str = None,
        times_as_micros: bool = True,
    ) -> dict:

        logging.info("Inferring avro schema from dataframe")
        # infer the schema using pandavro
        schema = pda.schema_infer(df=dataframe, times_as_micros=times_as_micros)

        # add custom schema name if exists (by default "Root")
        if schemaname:
            schema["name"] = schemaname
        # add custom schema name if exists (by default, non-existent)
        if namespace:
            schema["namespace"] = namespace

        return AvroSchema().run(schema)


class AvroSchemaToFile(Task):
    """Export avro schema to file"""

    def __init__(
        self,
        avsc: dict = None,
        filename: str = None,
        filedir: str = ".",
        **kwargs: Any,
    ):
        self.avsc = avsc
        self.filename = filename
        self.filedir = filedir
        super().__init__(**kwargs)

    @defaults_from_attrs("avsc", "filename", "filedir")
    def run(self, avsc: dict, filename: str = None, filedir: str = ".") -> str:

        logging.info("Exporting avro schema to file")
        # infer the filename based on the avro schema name from the avro dict key:values
        if not filename:
            filename = avsc["name"]

        # create internal copy of avro dict so as to not otherwise interfere
        _avsc = avsc.copy()

        # remove additional keys fastavro places in dict
        for key in ["__named_schemas", "__fastavro_parsed"]:
            if key in _avsc.keys():
                _avsc.pop(key)

        filepath = f"{filedir}/{filename}.avsc"

        with open(filepath, "w", encoding="utf-8") as avro_file:
            avro_file.write(json.dumps(_avsc, indent=4))

        return filepath


class DFValidateSchemaArrow(Task):
    """Validate Pandas dataframe against arrow schema"""

    def __init__(
        self,
        **kwargs: Any,
    ):

        super().__init__(**kwargs)

    def run(self, dataframe: pd.DataFrame, arsc: pa.lib.Schema) -> bool:

        self.logger.info("Validating dataframe against arrow schema")
        return arsc.equals(pa.Schema.from_pandas(dataframe))


class DFValidateSchemaAvro(Task):
    """Validate Pandas dataframe against avro schema dict"""

    def __init__(
        self,
        dataframe: pd.DataFrame = None,
        avsc: dict = None,
        **kwargs: Any,
    ):
        self.dataframe = dataframe
        self.avsc = avsc
        super().__init__(**kwargs)

    @defaults_from_attrs("dataframe", "avsc")
    def run(self, dataframe: pd.DataFrame, avsc: dict) -> bool:

        self.logger.info("Validating dataframe against avro schema dict.")
        return avro.validation.validate_many(
            records=dataframe.replace(pd.NA, "").to_dict(orient="records"), schema=avsc
        )
