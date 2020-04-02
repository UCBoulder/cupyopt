import pandas as pd
import pysftp
import datetime
import logging
import prefect
import os
import tempfile

from typing import Any, Optional
from prefect import Task
from prefect.utilities.tasks import defaults_from_attrs
from box import Box
import pandera as pa


class PdBoxedDataFrameToCSV(Task):
    """
    Exports boxed dataframes using temporary dicectory and names from a Dataframe Box
    
    Return a Box containing filepaths for each Dataframe
    """

    def __init__(
        self, dataframe: Box, config_box: Box, index=False, header=True, **kwargs: Any
    ):
        self.dataframe = dataframe
        self.config_box = config_box
        self.index = index
        self.header = header
        super().__init__(**kwargs)

    @defaults_from_attrs("dataframe", "config_box" "index", "header")
    def run(
        self,
        dataframe: Box,
        config_box: Box,
        index=True,
        header=True,
        **format_kwargs: Any
    ) -> Box:
        with prefect.context(**format_kwargs) as data:

            filepaths = Box()
            for name, df in dataframe.dataframe.items():
                filepath = config_box.extracttempdir + "/" + name + ".csv"
                filepaths += {name: filepath}
                df.to_csv(filepath, index=index, header=header)

            return filepaths