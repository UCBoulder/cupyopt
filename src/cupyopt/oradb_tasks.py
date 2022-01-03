""" oracle database related Prefect tasks """

import os
from typing import Any

import pandas as pd
import sqlalchemy
from box import Box
from prefect import Task
from prefect.utilities.tasks import defaults_from_attrs
from sqlalchemy import create_engine

# pylint: disable=arguments-differ


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
    def run(self, config_box: Box = None, is_sid: bool = None) -> str:

        if not isinstance(config_box, Box):
            raise ValueError("The configuration object must be a Box")

        # This will help encoding since most of our dbs are UTF8
        os.environ["NLS_LANG"] = ".AL32UTF8"

        self.logger.info(
            "Creating DB engine for %s@%s",
            config_box["database"],
            config_box["hostname"],
        )

        if config_box.port:
            port = config_box["port"]
        else:
            port = "1521"

        if is_sid:
            database = config_box["database"]
        else:
            database = f'?service_name={config_box["database"]}'

        if config_box.get("proxy_username"):
            username = f'{config_box["username"]}[{config_box["proxy_username"]}]'
        else:
            username = config_box["username"]

        # oracle connection string which will have
        # values replaced based on credentials provided from above json file
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

        # create the database connection engine using sqlalchemy library and
        # formated oracle_connection_string. Local SID instances can't support
        # identifiers longer than 128.
        if is_sid:
            engine = create_engine(oracle_connection_string, max_identifier_length=128)
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
        **kwargs: Any,
    ):
        self.select_stmt = select_stmt
        self.engine = engine
        super().__init__(**kwargs)

    @defaults_from_attrs("select_stmt", "engine")
    def run(
        self,
        select_stmt: str = None,
        engine: sqlalchemy.engine.base.Engine = None,
    ) -> pd.DataFrame:

        self.logger.info(
            "Running select statement using SQLAlchemy engine to Pandas DataFrame."
        )

        dataframe = pd.read_sql(sql=select_stmt, con=engine)

        return dataframe
