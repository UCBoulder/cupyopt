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
    def run(self, config_box: Box = None, is_sid: bool = None, **format_kwargs: Any) -> str:
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
                database = "service_name={}".format(config_box["database"])

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
        select_stmt: str,
        engine: sqlalchemy.engine.base.Engine,
        index_col=None,
        coerce_float=True,
        params=None,
        parse_dates=None,
        columns=None,
        chunksize=None,
        **kwargs: Any
    ):
        self.select_stmt = select_stmt
        self.engine = engine
        self.index_col = index_col
        self.coerce_float = coerce_float
        self.params = params
        self.parse_dates = parse_dates
        self.columns = columns
        self.chunksize = chunksize
        super().__init__(**kwargs)

    @defaults_from_attrs(
        "select_stmt",
        "engine",
        "index_col",
        "coerce_float",
        "params",
        "parse_dates",
        "columns",
        "chunksize",
    )
    def run(
        self,
        select_stmt: str,
        engine: sqlalchemy.engine.base.Engine,
        index_col=None,
        coerce_float=True,
        params=None,
        parse_dates=None,
        columns=None,
        chunksize=None,
        **format_kwargs: Any
    ) -> pd.DataFrame:

        with prefect.context(**format_kwargs) as data:

            self.logger.info(
                "Running select statement using SQLAlchemy engine to Pandas DataFrame."
            )

            df = pd.read_sql(
                sql=select_stmt,
                con=engine,
                index_col=index_col,
                coerce_float=coerce_float,
                params=params,
                parse_dates=parse_dates,
                columns=columns,
                chunksize=chunksize,
            )

        return df


class ORADBSelectToBoxedDataFrame(Task):
    """
    Runs select statement against database using SQLAlchemy engine.
    
    Return a Box containing a Pandas DataFrame with data collected from SQL statement. 
    """

    def __init__(
        self,
        config_box: Box,
        engine: sqlalchemy.engine.base.Engine,
        index_col=None,
        coerce_float=True,
        params=None,
        parse_dates=None,
        columns=None,
        chunksize=None,
        **kwargs: Any
    ):
        self.config_box = config_box
        self.engine = engine
        self.index_col = index_col
        self.coerce_float = coerce_float
        self.params = params
        self.parse_dates = parse_dates
        self.columns = columns
        self.chunksize = chunksize
        super().__init__(**kwargs)

    @defaults_from_attrs(
        "config_box",
        "engine",
        "index_col",
        "coerce_float",
        "params",
        "parse_dates",
        "columns",
        "chunksize",
    )
    def run(
        self,
        config_box: Box,
        engine: sqlalchemy.engine.base.Engine,
        index_col=None,
        coerce_float=True,
        params=None,
        parse_dates=None,
        columns=None,
        chunksize=None,
        **format_kwargs: Any
    ) -> Box:

        with prefect.context(**format_kwargs) as data:

            result_box = Box()
            """
            Expect the format of the config_box for this task to be as follows:
            {dataframe_name: select_query}
            """
            for name, query in config_box.items():
                self.logger.info(
                    "Running select statement using SQLAlchemy engine to Boxed Pandas DataFrame for dataframe: {}.".format(
                        name
                    )
                )
                df = pd.read_sql(
                    sql=query,
                    con=engine,
                    index_col=index_col,
                    coerce_float=coerce_float,
                    params=params,
                    parse_dates=parse_dates,
                    columns=columns,
                    chunksize=chunksize,
                )
                result_box += {"dataframe": {name: df}}

        return result_box