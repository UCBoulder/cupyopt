""" Tests dataframe nuggets """

import pandas as pd
from cupyopt.dataframe_tasks import DFColumnUpdate

# pylint: disable=duplicate-code


def test_dfcolumnupdate():
    """Tests dataframe nugget: pd_colupdate"""
    # create a test dataframe
    sample_df = pd.DataFrame({"A": [1, 2, 3], "B": [4, 5, 6]})

    # create a dict to be used for renaming and filtering columns in df
    coldict = {"A": "tiger"}

    # run task to rename and filter the dataframe
    new_df = DFColumnUpdate().run(sample_df, coldict)

    # test whether we have a single filtered and renamed column remaining in new df
    assert list(new_df.columns) == ["tiger"]
