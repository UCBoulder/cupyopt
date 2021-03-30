""" Schema functions """
import json
from typing import Union
import logging

import fastavro as avro
import pandas as pd
import pandavro as pda

logger = logging.getLogger(__name__)  # pylint: disable=C0103


def avro_schema(avsc: Union[dict, str]) -> dict:
    """ Create avro schema from dictionary or filepath string """
    # if a dictionary type, parse from dict
    logging.info("Parsing avro schema")
    if isinstance(avsc, dict):
        avsc = avro.schema.parse_schema(avsc)

    # if a str type, load from file
    elif isinstance(avsc, str):
        avsc = avro.schema.load_schema(avsc)

    return avsc


def infer_df_avro_schema(
    dataframe: pd.DataFrame,
    name: str = None,
    namespace: str = None,
    times_as_micros: bool = True,
) -> dict:
    """ Infer avro schema from pandas dataframe """
    logging.info("Inferring avro schema from dataframe")
    # infer the schema using pandavro
    schema = pda.schema_infer(dataframe=dataframe, times_as_micros=times_as_micros)

    # add custom schema name if exists (by default "Root")
    if name:
        schema["name"] = name
    # add custom schema name if exists (by default, non-existent)
    if namespace:
        schema["namespace"] = namespace

    return avro_schema(schema)


def avro_schema_to_file(avsc: dict, filename: str = None, filedir: str = "./") -> str:
    """ Export avro schema to file """
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

    filepath = "{}{}.avsc".format(filedir, filename)

    with open(filepath, "w") as avro_file:
        avro_file.write(json.dumps(_avsc, indent=4))

    return filepath
