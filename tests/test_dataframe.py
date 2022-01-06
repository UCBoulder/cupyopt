""" Tests dataframe nuggets """

import pandas as pd
from cupyopt.nuggets.dataframe import pd_colupdate


def test_pd_colupdate():
    """Tests dataframe nugget: pd_colupdate"""
    # create a test dataframe
    sample_df = pd.DataFrame({"A": [1, 2, 3], "B": [4, 5, 6]})

    # create a dict to be used for renaming and filtering columns in df
    coldict = {"A": "tiger"}

    # run task to rename and filter the dataframe
    new_df = pd_colupdate(dataframe=sample_df, coldict=coldict)

    # test whether we have a single filtered and renamed column remaining in new df
    assert list(new_df.columns) == ["tiger"]
