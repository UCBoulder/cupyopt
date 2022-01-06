""" SFTP related Prefect tasks """
import datetime
import os
from typing import Any

import pandas as pd
import prefect
import pysftp
from box import Box
from prefect import Task

# pylint: disable=arguments-differ, no-member, logging-too-many-args


class SFTPExists(Task):
    """
    Checks filename from FTP server

    Return True if found, otherwise False
    """

    def __init__(self, **kwargs: Any):
        super().__init__(**kwargs)

    def run(
        self,
        workfile: str,
        config_box: Box,
        cnopts: pysftp.CnOpts = None,
        **format_kwargs: Any,
    ) -> str:
        with prefect.context(**format_kwargs) as data:

            if data.get("parameters"):
                if data.parameters.get("cnopts"):
                    cnopts = data.parameters["cnopts"]

            # We have to handle either a password or a key
            if config_box.get("private_key_path"):
                private_key = config_box["private_key_path"]
                if config_box.get("private_key_passphrase"):
                    # has a passphrase, use it
                    private_key_passphrase = config_box["private_key_passphrase"]
                    sftp = pysftp.Connection(
                        host=config_box["hostname"],
                        username=config_box["username"],
                        private_key=private_key,
                        private_key_pass=private_key_passphrase,
                        cnopts=cnopts,
                    )
                else:
                    sftp = pysftp.Connection(
                        host=config_box["hostname"],
                        username=config_box["username"],
                        private_key=private_key,
                        cnopts=cnopts,
                    )
            elif config_box.get("password"):
                password = config_box["password"]
                sftp = pysftp.Connection(
                    host=config_box["hostname"],
                    username=config_box["username"],
                    password=password,
                    cnopts=cnopts,
                )

            try:
                with sftp.cd(config_box["target_dir"]):
                    result = sftp.exists(workfile)
            finally:
                sftp.close()

            return result


class SFTPGet(Task):
    """
    Fetch filename from FTP server

    Return a file_location_name
    """

    def __init__(self, **kwargs: Any):
        super().__init__(**kwargs)

    def run(
        self,
        workfile: str,
        config_box: Box,
        cnopts: pysftp.CnOpts = None,
        tempfolderpath: str = None,
        **format_kwargs: Any,
    ) -> str:
        with prefect.context(**format_kwargs) as data:

            if data.get("parameters"):
                if data.parameters.get("cnopts"):
                    cnopts = data.parameters["cnopts"]
                if data.parameters.get("cache"):
                    tempfolderpath = data.parameters["cache"]

            localtmpfile = os.path.join(tempfolderpath, workfile)
            self.logger.debug("Working on {}", os.path.join(tempfolderpath, workfile))

            if config_box.get("private_key_path"):
                private_key = config_box["private_key_path"]
                if config_box.get("private_key_passphrase"):
                    # has a passphrase, use it
                    private_key_passphrase = config_box["private_key_passphrase"]
                    sftp = pysftp.Connection(
                        host=config_box["hostname"],
                        username=config_box["username"],
                        private_key=private_key,
                        private_key_pass=private_key_passphrase,
                        cnopts=cnopts,
                    )
                else:
                    sftp = pysftp.Connection(
                        host=config_box["hostname"],
                        username=config_box["username"],
                        private_key=private_key,
                        cnopts=cnopts,
                    )
            elif config_box.get("password"):
                password = config_box["password"]
                sftp = pysftp.Connection(
                    host=config_box["hostname"],
                    username=config_box["username"],
                    password=password,
                    cnopts=cnopts,
                )

            try:
                with sftp.cd(config_box["target_dir"]):
                    sftp.get(workfile, localpath=localtmpfile, preserve_mtime=False)
            finally:
                sftp.close()

            self.logger.info("SFTPGet {}", localtmpfile)

            return localtmpfile


class SFTPPut(Task):
    """
    Put a file on the FTP server

    Leave remotepath off, or None and the workfile and the remote file are the same.
    """

    def __init__(self, **kwargs: Any):
        super().__init__(**kwargs)

    def run(
        self,
        workfile: str,
        config_box: Box,
        cnopts: pysftp.CnOpts = None,
        remotepath: str = None,
        **format_kwargs: Any,
    ):
        with prefect.context(**format_kwargs) as data:

            if data.get("parameters"):
                if data.parameters.get("cnopts"):
                    cnopts = data.parameters["cnopts"]

            if config_box.get("private_key_path"):
                private_key = config_box["private_key_path"]
                if config_box.get("private_key_passphrase"):
                    # has a passphrase, use it
                    private_key_passphrase = config_box["private_key_passphrase"]
                    sftp = pysftp.Connection(
                        host=config_box["hostname"],
                        username=config_box["username"],
                        private_key=private_key,
                        private_key_pass=private_key_passphrase,
                        cnopts=cnopts,
                    )
                else:
                    sftp = pysftp.Connection(
                        host=config_box["hostname"],
                        username=config_box["username"],
                        private_key=private_key,
                        cnopts=cnopts,
                    )
            elif config_box.get("password"):
                password = config_box["password"]
                sftp = pysftp.Connection(
                    host=config_box["hostname"],
                    username=config_box["username"],
                    password=password,
                    cnopts=cnopts,
                )

            try:
                if not sftp.isdir(config_box["target_dir"]):
                    sftp.mkdir(config_box["target_dir"])

                with sftp.cd(config_box["target_dir"]):
                    sftp.put(workfile, preserve_mtime=False, remotepath=remotepath)
            finally:
                sftp.close()

            self.logger.info("SFTPPut {}", workfile)


class SFTPRemove(Task):
    """
    Remove file from the FTP server
    """

    def __init__(self, **kwargs: Any):
        super().__init__(**kwargs)

    def run(
        self,
        workfile: str,
        config_box: Box,
        cnopts: pysftp.CnOpts = None,
        **format_kwargs: Any,
    ):
        with prefect.context(**format_kwargs) as data:

            self.logger.debug("Attempting to remove ", workfile)

            if data.get("parameters"):
                if data.parameters.get("cnopts"):
                    cnopts = data.parameters["cnopts"]

            if config_box.get("private_key_path"):
                private_key = config_box["private_key_path"]
                if config_box.get("private_key_passphrase"):
                    # has a passphrase, use it
                    private_key_passphrase = config_box["private_key_passphrase"]
                    sftp = pysftp.Connection(
                        host=config_box["hostname"],
                        username=config_box["username"],
                        private_key=private_key,
                        private_key_pass=private_key_passphrase,
                        cnopts=cnopts,
                    )
                else:
                    sftp = pysftp.Connection(
                        host=config_box["hostname"],
                        username=config_box["username"],
                        private_key=private_key,
                        cnopts=cnopts,
                    )
            elif config_box.get("password"):
                password = config_box["password"]
                sftp = pysftp.Connection(
                    host=config_box["hostname"],
                    username=config_box["username"],
                    password=password,
                    cnopts=cnopts,
                )

            # Pick out the oldest file in the dataframe
            try:
                with sftp.cd(config_box["target_dir"]):
                    sftp.remove(workfile)
            finally:
                sftp.close()

            # Read the file into a dataframe
            self.logger.info("SFTPRemove {}", workfile)


class SFTPRename(Task):
    """
    Rename (move) file on the FTP server
    """

    def __init__(self, **kwargs: Any):
        super().__init__(**kwargs)

    def run(
        self,
        config_box: Box,
        source="root",
        target="wip",
        cnopts: pysftp.CnOpts = None,
        **format_kwargs: Any,
    ):
        with prefect.context(**format_kwargs) as data:

            # This moves (renames) files around on the SFTP site.
            # It's helpful for managing "state" as the files are processed locally.

            if data.parameters.get("cnopts"):
                cnopts = data.parameters["cnopts"]

            if config_box.get("private_key_path"):
                private_key = config_box["private_key_path"]
                if config_box.get("private_key_passphrase"):
                    # has a passphrase, use it
                    private_key_passphrase = config_box["private_key_passphrase"]
                    sftp = pysftp.Connection(
                        host=config_box["hostname"],
                        username=config_box["username"],
                        private_key=private_key,
                        private_key_pass=private_key_passphrase,
                        cnopts=cnopts,
                    )
                else:
                    sftp = pysftp.Connection(
                        host=config_box["hostname"],
                        username=config_box["username"],
                        private_key=private_key,
                        cnopts=cnopts,
                    )
            elif config_box.get("password"):
                password = config_box["password"]
                sftp = pysftp.Connection(
                    host=config_box["hostname"],
                    username=config_box["username"],
                    password=password,
                    cnopts=cnopts,
                )

            # "root" is special
            remotesourcepath = (
                config_box["target_dir"]
                if source != "root"
                else os.path.join(config_box["target_dir"], source)
            )

            remotetargetpath = os.path.join(config_box["target_dir"], target)
            filename = os.path.basename(remotesourcepath)

            self.logger.debug(
                "Moving {} from {} to {}", filename, remotesourcepath, remotetargetpath
            )

            # Move the file from source to target on the SFTP
            try:
                with sftp.cd(config_box["target_dir"]):

                    if not sftp.isfile(os.path.join(remotesourcepath, filename)):
                        # Working in notebooks you might have already
                        # moved the file in another block
                        self.logger.warning(
                            "The file {} isn't in the remote folder.",
                            os.path.join(remotesourcepath, filename),
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
            finally:
                sftp.close()


class SFTPPoll(Task):
    """
    Polls for SFTP files
    """

    def __init__(self, **kwargs: Any):
        super().__init__(**kwargs)

    def run(
        self, config_box: Box, cnopts: pysftp.CnOpts = None, **format_kwargs: Any
    ) -> pd.DataFrame:
        with prefect.context(**format_kwargs) as data:

            if data.get("parameters"):
                if data.parameters.get("cnopts"):
                    cnopts = data.parameters["cnopts"]

            if config_box.get("private_key_path"):
                private_key = config_box["private_key_path"]
                if config_box.get("private_key_passphrase"):
                    # has a passphrase, use it
                    private_key_passphrase = config_box["private_key_passphrase"]
                    sftp = pysftp.Connection(
                        host=config_box["hostname"],
                        username=config_box["username"],
                        private_key=private_key,
                        private_key_pass=private_key_passphrase,
                        cnopts=cnopts,
                    )
                else:
                    sftp = pysftp.Connection(
                        host=config_box["hostname"],
                        username=config_box["username"],
                        private_key=private_key,
                        cnopts=cnopts,
                    )
            elif config_box.get("password"):
                password = config_box["password"]
                sftp = pysftp.Connection(
                    host=config_box["hostname"],
                    username=config_box["username"],
                    password=password,
                    cnopts=cnopts,
                )

            files_data = []

            try:
                with sftp.cd(config_box["target_dir"]):
                    for dir_file in sftp.listdir():

                        # Extra dirs like wip and done require us to
                        # check if we're just looking at files
                        if sftp.isfile(dir_file):

                            # get the dates from the ftp site itself
                            sftpattrs = sftp.stat(dir_file)

                            files_data.append(
                                {
                                    "File Name": dir_file,
                                    "MTime": datetime.datetime.fromtimestamp(
                                        sftpattrs.st_mtime
                                    ),
                                }
                            )
            finally:
                sftp.close()

            files_df = pd.DataFrame(files_data, columns=["File Name", "MTime"])

            self.logger.info("Found {} files to process.", len(files_df.index))
            return files_df


class DFGetOldestFile(Task):
    """
    Pick the oldest file off the top of the given dataframe.
    The dataframe requires columns called 'File Name' and 'MTime'

    Includes a search string to filter the list

    Returns a filename to fetch.
    """

    def __init__(self, **kwargs: Any):
        super().__init__(**kwargs)

    def run(self, files_df: pd.DataFrame, regex_search: str) -> str:

        if "MTime" not in files_df:
            raise ValueError("The MTime column is missing from the dataframe.")

        if "File Name" not in files_df:
            raise ValueError("The 'File Name' column is missing from the dataframe.")

        if len(files_df.index) == 0:
            self.logger.debug("The given DataFrame is empty.")
            return None

        if regex_search:
            files_df = files_df[
                files_df["File Name"].str.contains(regex_search, regex=True)
            ]

        files_df = files_df.sort_values(by="MTime")

        # Fetch the oldest file from the frame and bring it to a local temp file.
        workfile = files_df.iloc[0]["File Name"]
        self.logger.info("Found oldest file, {}", workfile)
        return workfile
