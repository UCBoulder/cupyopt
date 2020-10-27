import pytest
import sys
import os
sys.path.append(os.getcwd())
import pandas as pd
from src.cupyopt.pandas_tasks import PdColRenameAndFilter

def test_PdColRenameAndFilter():
	# create a test dataframe
	df = pd.DataFrame({"A": [1, 2, 3], "B": [4, 5, 6]})
	# create a dict to be used for renaming and filtering columns in df
	coldict = {"A":"tiger"}
	# instantiate the task class
	task = PdColRenameAndFilter(df=df,coldict=coldict)
	# run task to rename and filter the dataframe
	new_df = task.run()
	# test whether we have a single filtered and renamed column remaining in new df
	assert(list(new_df.columns) == ["tiger"])