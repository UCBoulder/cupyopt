import json
import pandas as pd
import fastavro as avro
import pandavro as pda
from typing import Union

def avro_schema(avsc: Union[dict, str]) -> dict:

    # if a dictionary type, parse from dict
    if type(avsc) == dict:
        return avro.schema.parse_schema(avsc)
    # if a str type, load from file
    elif type(avsc) == str:
        return avro.schema.load_schema(avsc)


def infer_df_avro_schema(
    df: pd.DataFrame,
    name: str = None,
    namespace: str = None,
    times_as_micros: bool = True,
) -> dict:

    # infer the schema using pandavro
    schema = pda.__schema_infer(df=df, times_as_micros=times_as_micros)

    # add custom schema name if exists (by default "Root")
    if name:
        schema["name"] = name
    # add custom schema name if exists (by default, non-existent)
    if namespace:
        schema["namespace"] = namespace

    return avro_schema(schema)


def avro_schema_to_file(avsc: dict, filename: str = None, filedir: str = "./"):

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

    with open(filepath, "w") as f:
        f.write(json.dumps(_avsc, indent=4))

    return filepath
