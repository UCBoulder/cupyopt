""" Init for cupyopt base """
from .oradb_tasks import ORADBGetEngine, ORADBSelectToDataFrame
from .sftp_tasks import (
    DFGetOldestFile,
    SFTPExists,
    SFTPGet,
    SFTPPoll,
    SFTPPut,
    SFTPRemove,
)
from .task_factory import ptask
