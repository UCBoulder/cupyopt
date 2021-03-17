import datetime
import logging
import os
import tempfile
from typing import Any

import pandas as pd
import prefect
import pysftp
from box import Box
from prefect import Task
from prefect.utilities.tasks import defaults_from_attrs


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
        **format_kwargs: Any
    ) -> str:
        with prefect.context(**format_kwargs) as data:

            if data.get("parameters"):
                if data.parameters.get("cnopts"):
                    cnopts = data.parameters["cnopts"]

            hostname = config_box["hostname"]
            username = config_box["username"]
            remoterootpath = config_box["target_dir"]

            # We have to handle either a password or a key
            if config_box.get("private_key_path"):
                private_key = config_box["private_key_path"]
                if config_box.get("private_key_passphrase"):
                    # has a passphrase, use it
                    private_key_passphrase = config_box["private_key_passphrase"]
                    sftp = pysftp.Connection(
                        host=hostname,
                        username=username,
                        private_key=private_key,
                        private_key_pass=private_key_passphrase,
                        cnopts=cnopts,
                    )
                else:
                    sftp = pysftp.Connection(
                        host=hostname,
                        username=username,
                        private_key=private_key,
                        cnopts=cnopts,
                    )
            elif config_box.get("password"):
                password = config_box["password"]
                sftp = pysftp.Connection(
                    host=hostname, username=username, password=password, cnopts=cnopts
                )

            try:
                with sftp.cd(remoterootpath):
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
        **format_kwargs: Any
    ) -> str:
        with prefect.context(**format_kwargs) as data:

            hostname = config_box["hostname"]
            username = config_box["username"]
            remoterootpath = config_box["target_dir"]

            if data.get("parameters"):
                if data.parameters.get("cnopts"):
                    cnopts = data.parameters["cnopts"]
                if data.parameters.get("cache"):
                    tempfolderpath = data.parameters["cache"]

            localtmpfile = os.path.join(tempfolderpath, workfile)
            self.logger.debug("Working on ", localtmpfile)

            if config_box.get("private_key_path"):
                private_key = config_box["private_key_path"]
                if config_box.get("private_key_passphrase"):
                    # has a passphrase, use it
                    private_key_passphrase = config_box["private_key_passphrase"]
                    sftp = pysftp.Connection(
                        host=hostname,
                        username=username,
                        private_key=private_key,
                        private_key_pass=private_key_passphrase,
                        cnopts=cnopts,
                    )
                else:
                    sftp = pysftp.Connection(
                        host=hostname,
                        username=username,
                        private_key=private_key,
                        cnopts=cnopts,
                    )
            elif config_box.get("password"):
                password = config_box["password"]
                sftp = pysftp.Connection(
                    host=hostname, username=username, password=password, cnopts=cnopts
                )

            try:
                with sftp.cd(remoterootpath):
                    sftp.get(workfile, localpath=localtmpfile, preserve_mtime=False)
            finally:
                sftp.close()

            # Read the file into a dataframe
            self.logger.info("SFTPGet {}".format(localtmpfile))
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
        **format_kwargs: Any
    ):
        with prefect.context(**format_kwargs) as data:

            hostname = config_box["hostname"]
            username = config_box["username"]
            remoterootpath = config_box["target_dir"]

            if data.get("parameters"):
                if data.parameters.get("cnopts"):
                    cnopts = data.parameters["cnopts"]

            if config_box.get("private_key_path"):
                private_key = config_box["private_key_path"]
                if config_box.get("private_key_passphrase"):
                    # has a passphrase, use it
                    private_key_passphrase = config_box["private_key_passphrase"]
                    sftp = pysftp.Connection(
                        host=hostname,
                        username=username,
                        private_key=private_key,
                        private_key_pass=private_key_passphrase,
                        cnopts=cnopts,
                    )
                else:
                    sftp = pysftp.Connection(
                        host=hostname,
                        username=username,
                        private_key=private_key,
                        cnopts=cnopts,
                    )
            elif config_box.get("password"):
                password = config_box["password"]
                sftp = pysftp.Connection(
                    host=hostname, username=username, password=password, cnopts=cnopts
                )

            try:
                if not sftp.isdir(remoterootpath):
                    sftp.mkdir(remoterootpath)

                with sftp.cd(remoterootpath):
                    sftp.put(workfile, preserve_mtime=False, remotepath=remotepath)
            finally:
                sftp.close()

            self.logger.info("SFTPPut {}".format(workfile))


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
        **format_kwargs: Any
    ):
        with prefect.context(**format_kwargs) as data:

            self.logger.debug("Attempting to remove ", workfile)

            hostname = config_box["hostname"]
            username = config_box["username"]
            remoterootpath = config_box["target_dir"]

            if data.get("parameters"):
                if data.parameters.get("cnopts"):
                    cnopts = data.parameters["cnopts"]

            if config_box.get("private_key_path"):
                private_key = config_box["private_key_path"]
                if config_box.get("private_key_passphrase"):
                    # has a passphrase, use it
                    private_key_passphrase = config_box["private_key_passphrase"]
                    sftp = pysftp.Connection(
                        host=hostname,
                        username=username,
                        private_key=private_key,
                        private_key_pass=private_key_passphrase,
                        cnopts=cnopts,
                    )
                else:
                    sftp = pysftp.Connection(
                        host=hostname,
                        username=username,
                        private_key=private_key,
                        cnopts=cnopts,
                    )
            elif config_box.get("password"):
                password = config_box["password"]
                sftp = pysftp.Connection(
                    host=hostname, username=username, password=password, cnopts=cnopts
                )

            # Pick out the oldest file in the dataframe
            try:
                with sftp.cd(remoterootpath):
                    sftp.remove(workfile)
            finally:
                sftp.close()

            # Read the file into a dataframe
            self.logger.info("SFTPRemove {}".format(workfile))


class SFTPRename(Task):
    """
    Rename (move) file on the FTP server
    """

    def __init__(self, **kwargs: Any):
        super().__init__(**kwargs)

    def run(
        self,
        workfile: str,
        config_box: Box,
        source="root",
        target="wip",
        cnopts: pysftp.CnOpts = None,
        **format_kwargs: Any
    ):
        with prefect.context(**format_kwargs) as data:

            # This moves (renames) files around on the SFTP site.
            # It's helpful for managing "state" as the files are processed locally.

            hostname = config["hostname"]
            username = config["username"]
            remoterootpath = config["target_dir"]

            if data.get("parameters"):
                if data.parameters.get("cnopts"):
                    cnopts = data.parameters["cnopts"]

            if config_box.get("private_key_path"):
                private_key = config_box["private_key_path"]
                if config_box.get("private_key_passphrase"):
                    # has a passphrase, use it
                    private_key_passphrase = config_box["private_key_passphrase"]
                    sftp = pysftp.Connection(
                        host=hostname,
                        username=username,
                        private_key=private_key,
                        private_key_pass=private_key_passphrase,
                        cnopts=cnopts,
                    )
                else:
                    sftp = pysftp.Connection(
                        host=hostname,
                        username=username,
                        private_key=private_key,
                        cnopts=cnopts,
                    )
            elif config_box.get("password"):
                password = config_box["password"]
                sftp = pysftp.Connection(
                    host=hostname, username=username, password=password, cnopts=cnopts
                )

            # "root" is special
            if source == "root":
                remotesourcepath = remoterootpath
            else:
                remotesourcepath = os.path.join(remoterootpath, source)

            remotetargetpath = os.path.join(remoterootpath, target)
            logger.debug(
                "Moving %s from %s to %s"
                % (filename, remotesourcepath, remotetargetpath)
            )

            # Move the file from source to target on the SFTP
            try:
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
            finally:
                sftp.close()


class SFTPPoll(Task):
    def __init__(self, **kwargs: Any):
        super().__init__(**kwargs)

    def run(
        self, config_box: Box, cnopts: pysftp.CnOpts = None, **format_kwargs: Any
    ) -> pd.DataFrame:
        with prefect.context(**format_kwargs) as data:

            hostname = config_box["hostname"]
            username = config_box["username"]
            remoterootpath = config_box["target_dir"]

            if data.get("parameters"):
                if data.parameters.get("cnopts"):
                    cnopts = data.parameters["cnopts"]

            if config_box.get("private_key_path"):
                private_key = config_box["private_key_path"]
                if config_box.get("private_key_passphrase"):
                    # has a passphrase, use it
                    private_key_passphrase = config_box["private_key_passphrase"]
                    sftp = pysftp.Connection(
                        host=hostname,
                        username=username,
                        private_key=private_key,
                        private_key_pass=private_key_passphrase,
                        cnopts=cnopts,
                    )
                else:
                    sftp = pysftp.Connection(
                        host=hostname,
                        username=username,
                        private_key=private_key,
                        cnopts=cnopts,
                    )
            elif config_box.get("password"):
                password = config_box["password"]
                sftp = pysftp.Connection(
                    host=hostname, username=username, password=password, cnopts=cnopts
                )

            files_data = []

            try:
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
            finally:
                sftp.close()

            filesdf = pd.DataFrame(files_data, columns=["File Name", "MTime"])

            self.logger.info("Found {} files to process.".format(len(filesdf.index)))
            return filesdf


class DFGetOldestFile(Task):
    def __init__(self, **kwargs: Any):
        super().__init__(**kwargs)

    def run(
        self, files_df: pd.DataFrame, regex_search: str, **format_kwargs: Any
    ) -> pd.DataFrame:
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
                raise ValueError(
                    "The 'File Name' column is missing from the dataframe."
                )

            if len(files_df.index) == 0:
                self.logger.debug("The given DataFrame is empty.")
                return None

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
