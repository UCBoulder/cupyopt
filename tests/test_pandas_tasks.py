import os
import sys

import pytest

sys.path.append(os.getcwd())

import pandas as pd
from src.cupyopt.nuggets.dataframe import pd_colupdate


def test_pd_colupdate():
    # create a test dataframe
    df = pd.DataFrame({"A": [1, 2, 3], "B": [4, 5, 6]})

    # create a dict to be used for renaming and filtering columns in df
    coldict = {"A": "tiger"}

    # run task to rename and filter the dataframe
    new_df = pd_colupdate(df=df, coldict=coldict)

    # test whether we have a single filtered and renamed column remaining in new df
    assert list(new_df.columns) == ["tiger"]
