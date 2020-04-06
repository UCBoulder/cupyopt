import pandas as pd
import pysftp
import datetime
import logging
import prefect
import os
import tempfile

from typing import Any
from prefect import Task
from prefect.utilities.tasks import defaults_from_attrs
from box import Box

class SFTPGet(Task):
    """
    Fetch filename from FTP server
    
    Return a file_location_name
    """    
    def __init__(self, **kwargs: Any):
        super().__init__(**kwargs)

    def run(self, workfile: str, config_box: Box, cnopts: pysftp.CnOpts, tempfolderpath: str, **format_kwargs: Any) -> str:    
        with prefect.context(**format_kwargs) as data:
            
            key_file = config_box["private_key_path"]
            hostname = config_box["hostname"]
            username = config_box["username"]
            remoterootpath = config_box["target_dir"]

            localtmpfile = os.path.join(tempfolderpath, workfile)
            self.logger.debug("Working on ", localtmpfile)

            # Pick out the oldest file in the dataframe
            with pysftp.Connection(
                host=hostname, username=username, private_key=key_file, cnopts=cnopts
            ) as sftp:
                with sftp.cd(remoterootpath):
                    sftp.get(workfile, localpath=localtmpfile, preserve_mtime=False)

            # Read the file into a dataframe
            self.logger.info("SFTPGet {}".format(localtmpfile))
            return localtmpfile

class SFTPPut(Task):
    """
    Put a file on the FTP server
    """    
    def __init__(self, **kwargs: Any):
        super().__init__(**kwargs)

    def run(self, workfile: str, config_box: Box, cnopts: pysftp.CnOpts, tempfolderpath: str, **format_kwargs: Any):    
        with prefect.context(**format_kwargs) as data:
            
            key_file = config_box["private_key_path"]
            hostname = config_box["hostname"]
            username = config_box["username"]
            remoterootpath = config_box["target_dir"]

            localtmpfile = os.path.join(tempfolderpath, workfile)
            self.logger.debug("Working on ", localtmpfile)

            with pysftp.Connection(
                host=hostname, username=username, private_key=key_file, cnopts=cnopts
            ) as sftp:
                sftp.makedirs(tempfolderpath)
                with sftp.cd(remoterootpath):
                    sftp.put(workfile, preserve_mtime=False)

            self.logger.info("SFTPPut {}".format(localtmpfile))

class SFTPPutFromFilepathsBox(Task):
    """
    Put a files on the FTP server based on a filepaths Box
    """

    def __init__(
        self,
        filepaths: Box,
        config_box: Box,
        cnopts: pysftp.CnOpts,
        tempfolderpath: str,
        **kwargs: Any
    ):
        self.filepaths = filepaths
        self.config_box = config_box
        self.cnopts = cnopts
        self.tempfolderpath = tempfolderpath
        super().__init__(**kwargs)

    @defaults_from_attrs("filepaths", "config_box", "cnopts", "tempfolderpath")
    def run(
        self,
        filepaths: Box,
        config_box: Box,
        cnopts: pysftp.CnOpts,
        tempfolderpath: str,
        **format_kwargs: Any
    ):
        with prefect.context(**format_kwargs) as data:

            key_file = config_box["private_key_path"]
            hostname = config_box["hostname"]
            username = config_box["username"]
            remoterootpath = config_box["target_dir"]

            # Pick out the oldest file in the dataframe
            with pysftp.Connection(
                host=hostname, username=username, private_key=key_file, cnopts=cnopts
            ) as sftp:
                #sftp.makedirs(tempfolderpath)
                with sftp.cd(remoterootpath):
                    for name, filepath in filepaths.items():
                        localtmpfile = os.path.join(tempfolderpath, name)
                        self.logger.debug("Working on ", localtmpfile)
                        sftp.put(filepath, preserve_mtime=False)
                        self.logger.info("SFTPPut {}".format(localtmpfile))

            return localtmpfile

class SFTPRemove(Task):
    """
    Remove file from the FTP server
    """    
    def __init__(self, **kwargs: Any):
        super().__init__(**kwargs)

    def run(self, workfile: str, config_box: Box, cnopts: pysftp.CnOpts, **format_kwargs: Any):    
        with prefect.context(**format_kwargs) as data:
            
            key_file = config_box["private_key_path"]
            hostname = config_box["hostname"]
            username = config_box["username"]
            remoterootpath = config_box["target_dir"]

            self.logger.debug("Working on ", workfile)

            # Pick out the oldest file in the dataframe
            with pysftp.Connection(
                host=hostname, username=username, private_key=key_file, cnopts=cnopts
            ) as sftp:
                with sftp.cd(remoterootpath):
                    sftp.remove(workfile)

            # Read the file into a dataframe
            self.logger.info("SFTPRemove {}".format(workfile))

class SFTPRename(Task):
    """
    Rename (move) file on the FTP server
    """    
    def __init__(self, **kwargs: Any):
        super().__init__(**kwargs)

    def run(self, workfile: str, config_box: Box, cnopts: pysftp.CnOpts, source="root", target="wip", **format_kwargs: Any):    
        with prefect.context(**format_kwargs) as data:

                # This moves (renames) files around on the SFTP site.
                # It's helpful for managing "state" as the files are processed locally.

                key_file = config["private_key_path"]
                hostname = config["hostname"]
                username = config["username"]
                remoterootpath = config["target_dir"]

                # "root" is special
                if source == "root":
                    remotesourcepath = remoterootpath
                else:
                    remotesourcepath = os.path.join(remoterootpath, source)

                remotetargetpath = os.path.join(remoterootpath, target)
                logger.debug(
                    "Moving %s from %s to %s" % (filename, remotesourcepath, remotetargetpath)
                )

                # Move the file from source to target on the SFTP
                with pysftp.Connection(
                    host=hostname, username=username, private_key=key_file, cnopts=cnopts
                ) as sftp:
                    with sftp.cd(remoterootpath):

                        if not sftp.isfile(os.path.join(remotesourcepath, filename)):
                            # Working in notebooks you might have already moved the file in another block
                            logger.warning(
                                "The file %s isn't in the remote folder."
                                % os.path.join(remotesourcepath, filename)
                            )
                        else:
                            if not sftp.isdir(remotetargetpath):
                                sftp.mkdir(remotetargetpath)

                            if not sftp.isfile(os.path.join(remotetargetpath, filename)):
                                # The file might already be in DONE...
                                sftp.rename(
                                    os.path.join(remotesourcepath, filename),
                                    os.path.join(remotetargetpath, filename),
                                )

class SFTPPoll(Task):
    
    def __init__(self, **kwargs: Any):
        super().__init__(**kwargs)

    def run(self, config_box: Box, cnopts: pysftp.CnOpts, **format_kwargs: Any) -> pd.DataFrame:    
        with prefect.context(**format_kwargs) as data:

            key_file = config_box["private_key_path"]
            hostname = config_box["hostname"]
            username = config_box["username"]
            remoterootpath = config_box["target_dir"]           
            
            files_data = []

            with pysftp.Connection(
                host=hostname, username=username, private_key=key_file, cnopts=cnopts
            ) as sftp:
                with sftp.cd(remoterootpath):
                    for f in sftp.listdir():

                        # Extra dirs like wip and done require us to check if we're just looking at files
                        if sftp.isfile(f):

                            # get the dates from the ftp site itself
                            sftpattrs = sftp.stat(f)

                            files_data.append(
                                {
                                    "File Name": f,
                                    "MTime": datetime.datetime.fromtimestamp(
                                        sftpattrs.st_mtime
                                    ),
                                }
                            )

            filesdf = pd.DataFrame(files_data, columns=["File Name", "MTime"])

            self.logger.info("Found {} files to process.".format(len(filesdf.index)))
            return filesdf  

class DFGetOldestFile(Task):
    
    def __init__(self, **kwargs: Any):
        super().__init__(**kwargs)

    def run(self, files_df: pd.DataFrame, regex_search: str, **format_kwargs: Any) -> pd.DataFrame:    
        with prefect.context(**format_kwargs) as data:
            
            """
            Pick the oldest file off the top of the given dataframe.
            The dataframe requires columns called 'File Name' and 'MTime'

            Includes a search string to filter the list

            Returns a filename to fetch.
            """

            if "MTime" not in files_df:
                raise ValueError("The MTime column is missing from the dataframe.")

            if "File Name" not in files_df:
                raise ValueError("The 'File Name' column is missing from the dataframe.")

            if len(files_df.index) == 0:
                raise ValueError("The given DataFrame is empty.")

            if regex_search:
                files_df = files_df[
                    files_df["File Name"].str.contains(regex_search, regex=True)
                ]

            files_df = files_df.sort_values(by="MTime")

            if len(files_df.index) > 0:
                # Fetch the oldest file from the frame and bring it to a local temp file.
                workfile = files_df.iloc[0]["File Name"]
                self.logger.info("Found oldest file, {}".format(workfile))
                return workfile
