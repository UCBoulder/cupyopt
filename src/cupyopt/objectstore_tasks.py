""" object store functions """

import logging
import os
from typing import Any
from typing_extensions import Literal

import pandas as pd
import urllib3
from box import Box
from minio import Minio
from prefect import Task
from prefect.utilities.tasks import defaults_from_attrs

# pylint: disable=arguments-differ, too-many-arguments


class ObjstrClient(Task):
    """setup object storage client via minio"""

    def __init__(
        self,
        config_box: Box = None,
        http_client: urllib3.poolmanager.PoolManager = None,
        **kwargs: Any
    ):
        self.config_box = config_box
        self.http_client = http_client
        super().__init__(**kwargs)

    @defaults_from_attrs("config_box", "http_client")
    def run(
        self, config_box: Box, http_client: urllib3.poolmanager.PoolManager = None
    ) -> Minio:

        logging.info(
            "Creating object store client for endpoint %s.", config_box.endpoint
        )
        client = Minio(
            endpoint=config_box.endpoint,
            access_key=config_box.key,
            secret_key=config_box.secret,
            secure=True,
            http_client=http_client,
        )

        return client


class ObjstrMakeBucket(Task):
    """attempts to make a bucket if not already available"""

    def __init__(self, client: Minio = None, bucket_name: str = None, **kwargs: Any):
        self.client = client
        self.bucket_name = bucket_name

        super().__init__(**kwargs)

    @defaults_from_attrs("client", "bucket_name")
    def run(self, client: Minio, bucket_name: str):

        # make bucket if not exists
        if not client.bucket_exists(bucket_name):
            client.make_bucket(bucket_name)
            logging.info("Created bucket %s", bucket_name)
        else:
            logging.info("Bucket %s already exists, taking no actions.", bucket_name)


class ObjstrPut(Task):
    """put data as object in object store"""

    def __init__(
        self,
        client: Minio = None,
        bucket_name: str = None,
        object_name: str = None,
        data: object = None,
        length: int = -1,
        part_size: int = 5 * 1024 * 1024,
        **kwargs: Any
    ):

        self.client = client
        self.bucket_name = bucket_name
        self.object_name = object_name
        self.data = data
        self.length = length
        self.part_size = part_size

        super().__init__(**kwargs)

    @defaults_from_attrs(
        "bucket_name", "object_name", "data", "length", "part_size", "client"
    )
    def run(
        self,
        client: Minio,
        bucket_name: str,
        object_name: str,
        data: object,
        length: int = None,
        part_size: int = None,
    ):
        if not length:
            length = self.length

        if not part_size:
            part_size = self.part_size

        # upload file as object
        client.put_object(
            bucket_name=bucket_name,
            object_name=object_name,
            data=data,
            length=length,
            part_size=part_size,
        )

        logging.info("Put data under %s as %s", bucket_name, object_name)


class ObjstrGet(Task):
    """get object as data from object store"""

    def __init__(
        self,
        client: Minio = None,
        bucket_name: str = None,
        object_name: str = None,
        **kwargs: Any
    ):
        self.client = client
        self.bucket_name = bucket_name
        self.object_name = object_name

        super().__init__(**kwargs)

    @defaults_from_attrs(
        "client",
        "bucket_name",
        "object_name",
    )
    def run(self, client: Minio, bucket_name: str, object_name: str) -> Any:

        # get object as file
        data = client.get_object(
            bucket_name=bucket_name,
            object_name=object_name,
        )

        logging.info(
            "Retrieved object %s under %s",
            object_name,
            bucket_name,
        )
        return data


class ObjstrGetAsDF(Task):
    """get object and return as pandas.dataframe from object store"""

    def __init__(
        self,
        client: Minio = None,
        bucket_name: str = None,
        object_name: str = None,
        dftype: Literal["csv", "parquet", "excel"] = None,
        **kwargs: Any
    ):
        self.client = client
        self.bucket_name = bucket_name
        self.object_name = object_name
        self.dftype = dftype

        super().__init__(**kwargs)

    @defaults_from_attrs("client", "bucket_name", "object_name", "dftype")
    def run(
        self,
        client: Minio,
        bucket_name: str,
        object_name: str,
        dftype: Literal["csv", "parquet", "excel"],
        **kwargs: Any
    ) -> Any:

        # get object as file
        data = client.get_object(
            bucket_name=bucket_name,
            object_name=object_name,
        )

        logging.info(
            "Retrieved object %s under %s",
            object_name,
            bucket_name,
        )

        if dftype == "csv":
            pd_dataframe = pd.read_csv(filepath_or_buffer=data, **kwargs)
        elif dftype == "parquet":
            pd_dataframe = pd.read_parquet(filepath_or_buffer=data, **kwargs)

        return pd_dataframe


class ObjstrFPut(Task):
    """put file as object in object store"""

    def __init__(
        self,
        client: Minio = None,
        bucket_name: str = None,
        file_path: str = None,
        object_name: str = None,
        **kwargs: Any
    ):

        self.client = client
        self.bucket_name = bucket_name
        self.file_path = file_path
        self.object_name = object_name

        super().__init__(**kwargs)

    @defaults_from_attrs("client", "bucket_name", "file_path", "object_name")
    def run(
        self,
        client: Minio,
        bucket_name: str,
        file_path: str,
        object_name: str = None,
    ):

        # if no object_name is provided, default to file_path basename
        if not object_name:
            object_name = os.path.basename(file_path)

        # upload file as object
        client.fput_object(
            bucket_name=bucket_name,
            object_name=object_name,
            file_path=file_path,
        )

        logging.info("Put file %s under %s as %s", file_path, bucket_name, object_name)


class ObjstrFGet(Task):
    """get object as file from object store"""

    def __init__(
        self,
        client: Minio = None,
        bucket_name: str = None,
        object_name: str = None,
        file_path: str = None,
        **kwargs: Any
    ):
        self.client = client
        self.bucket_name = bucket_name
        self.object_name = object_name
        self.file_path = file_path

        super().__init__(**kwargs)

    @defaults_from_attrs("client", "bucket_name", "object_name", "file_path")
    def run(
        self, client: Minio, bucket_name: str, object_name: str, file_path: str
    ) -> str:

        # get object as file
        client.fget_object(
            bucket_name=bucket_name,
            object_name=object_name,
            file_path=file_path,
        )

        logging.info(
            "Retrieved object %s under %s as file %s",
            object_name,
            bucket_name,
            file_path,
        )

        return file_path
