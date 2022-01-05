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
from .objectstore_tasks import (
    Objstr_client,
    Objstr_make_bucket,
    Objstr_fput,
    Objstr_fget,
)
from .task_factory import ptask
