""" object store functions """

import logging
from typing import Any

from box import Box
from minio import Minio
from box import Box
from prefect import Task
from prefect.utilities.tasks import defaults_from_attrs
import urllib3


class Objstr_client(Task):
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


class Objstr_make_bucket(Task):
    """attempts to make a bucket if not already available"""

    def __init__(self, bucket_name: str = None, client: Minio = None, **kwargs: Any):
        self.bucket_name = bucket_name
        self.client = client
        super().__init__(**kwargs)

    @defaults_from_attrs("bucket_name", "client")
    def run(self, bucket_name: str, client: Minio):

        # make bucket if not exists
        if not client.bucket_exists(bucket_name):
            client.make_bucket(bucket_name)
            logging.info("Created bucket %s", bucket_name)
        else:
            logging.info("Bucket %s already exists, taking no actions.", bucket_name)


class Objstr_put(Task):
    """put data as object in object store"""

    def __init__(
        self,
        bucket_name: str = None,
        object_name: str = None,
        data: object = None,
        client: Minio = None,
        **kwargs: Any
    ):

        self.bucket_name = bucket_name
        self.object_name = object_name
        self.data = data
        self.client = client
        super().__init__(**kwargs)

    @defaults_from_attrs("bucket_name", "object_name", "data", "client")
    def run(self, bucket_name: str, object_name: str, data: object, client: Minio):

        # upload file as object
        client.put_object(
            bucket_name=bucket_name,
            object_name=object_name,
            data=data,
        )

        logging.info("Put data under %s as %s", bucket_name, object_name)


class Objstr_get(Task):
    """get object as data from object store"""

    def __init__(
        self,
        bucket_name: str = None,
        object_name: str = None,
        client: Minio = None,
        **kwargs: Any
    ):
        self.bucket_name = bucket_name
        self.object_name = object_name
        self.client = client
        super().__init__(**kwargs)

    @defaults_from_attrs("bucket_name", "object_name", "client")
    def run(
        self, bucket_name: str, object_name: str, file_path: str, client: Minio
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
        return data


class Objstr_fput(Task):
    """put file as object in object store"""

    def __init__(
        self,
        bucket_name: str = None,
        object_name: str = None,
        file_path: str = None,
        client: Minio = None,
        **kwargs: Any
    ):

        self.bucket_name = bucket_name
        self.object_name = object_name
        self.file_path = file_path
        self.client = client
        super().__init__(**kwargs)

    @defaults_from_attrs("bucket_name", "object_name", "file_path", "client")
    def run(self, bucket_name: str, object_name: str, file_path: str, client: Minio):

        # upload file as object
        client.fput_object(
            bucket_name=bucket_name,
            object_name=object_name,
            file_path=file_path,
        )

        logging.info("Put file %s under %s as %s", file_path, bucket_name, object_name)


class Objstr_fget(Task):
    """get object as file from object store"""

    def __init__(
        self,
        bucket_name: str = None,
        object_name: str = None,
        file_path: str = None,
        client: Minio = None,
        **kwargs: Any
    ):

        self.bucket_name = bucket_name
        self.object_name = object_name
        self.file_path = file_path
        self.client = client
        super().__init__(**kwargs)

    @defaults_from_attrs("bucket_name", "object_name", "file_path", "client")
    def run(self, bucket_name: str, object_name: str, file_path: str, client: Minio):

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
