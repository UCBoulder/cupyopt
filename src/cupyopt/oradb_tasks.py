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
from prefect.utilities.tasks import defaults_from_attrs
from box import Box


class ORADBGetEngine(Task):
    """
    Configure an sqlalchemy engine with cx_Oracle
    
    Return a sqlalchemy engine
    """

    def __init__(self, config_box: Box = None, is_sid: bool = None, **kwargs: Any):
        self.config_box = config_box
        self.is_sid = is_sid
        super().__init__(**kwargs)

    @defaults_from_attrs("config_box", "is_sid")
    def run(
        self, config_box: Box = None, is_sid: bool = None, **format_kwargs: Any
    ) -> str:
        with prefect.context(**format_kwargs) as data:

            if not type(config_box) == Box:
                raise ValueError("The configuration object must be a Box")

            # This will help encoding since most of our dbs are UTF8
            os.environ["NLS_LANG"] = ".AL32UTF8"

            self.logger.info(
                "Creating DB engine for {}@{}".format(
                    config_box["database"], config_box["hostname"]
                )
            )

            if config_box.port:
                port = config_box["port"]
            else:
                port = "1521"

            if is_sid:
                database = config_box["database"]
            else:
                database = "?service_name={}".format(config_box["database"])

            if config_box.get("proxy_username"):
                username = "{}[{}]".format(
                    config_box["username"], config_box["proxy_username"]
                )
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
                engine = create_engine(
                    oracle_connection_string, max_identifier_length=128
                )
            else:
                engine = create_engine(oracle_connection_string)

            return engine


class ORADBSelectToDataFrame(Task):
    """
    Runs select statement against database using SQLAlchemy engine.
    
    Return a Pandas DataFrame with data collected from SQL statement. 
    """

    def __init__(
        self,
        select_stmt: str = None,
        engine: sqlalchemy.engine.base.Engine = None,
        **kwargs: Any
    ):
        self.select_stmt = select_stmt
        self.engine = engine
        super().__init__(**kwargs)

    @defaults_from_attrs("select_stmt", "engine")
    def run(
        self,
        select_stmt: str = None,
        engine: sqlalchemy.engine.base.Engine = None,
        **format_kwargs: Any
    ) -> pd.DataFrame:

        with prefect.context(**format_kwargs) as data:

            self.logger.info(
                "Running select statement using SQLAlchemy engine to Pandas DataFrame."
            )

            df = pd.read_sql(sql=select_stmt, con=engine)

        return df
