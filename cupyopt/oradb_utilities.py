# +
import pandas as pd
import datetime
import logging
import prefect
import os
import tempfile
import cx_Oracle
import sqlalchemy

from typing import Any
from sqlalchemy.dialects.oracle import VARCHAR2
from sqlalchemy import types, create_engine
from prefect import Task
from box import Box


# -

class ORADBGetEngine(Task):
    """
    Configure an sqlalchemy engine with cx_Oracle
    
    Return a sqlalchemy engine
    """    
    def __init__(self, **kwargs: Any):
        super().__init__(**kwargs)

    def run(self, config_box: Box, is_sid=False, **format_kwargs: Any) -> str:    
        with prefect.context(**format_kwargs) as data:
            
            if not type(config_box) == Box:
                raise ValueError("The configuration object must be a Box")
            
            #This will help encoding since most of our dbs are UTF8
            os.environ["NLS_LANG"] = ".AL32UTF8"

            self.logger.info(
                "Creating DB engine for {}@{}".format(config_box["database"], config_box["hostname"])
            )

            if config_box.port:
                port = config_box['port']
            else:
                port = "1521"
            
            if is_sid:
                database = config_box["database"]
            else:
                database = "service_name={}".format(config_box['database'])

            if config_box.get('proxy_username'):
                username = "{}[{}]".format(config_box["username"], config_box['proxy_username'])
            else:
                username = config_box["username"]

            # oracle connection string which will have values replaced based on credentials provided from above json file
            oracle_connection_string = (
                "oracle+cx_oracle://{username}:{password}@{hostname}:{port}/{database}"
            )

            oracle_connection_string = oracle_connection_string.format(
                username=username,
                password=config_box["password"],
                hostname=config_box["hostname"],
                port=port,
                database=database,
            )
            
            # create the database connection engine using sqlalchemy library and formated oracle_connection_string
            # I've found that our SID instances can't support identifiers longer than 128.
            if is_sid:
                engine = create_engine(oracle_connection_string,max_identifier_length=128)
            else:
                engine = create_engine(oracle_connection_string)

            return engine


