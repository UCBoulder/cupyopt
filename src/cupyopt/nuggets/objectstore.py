""" object store functions """

import logging

from box import Box
from minio import Minio


def objstr_client(config: Box) -> Minio:
    """setup object storage client via minio"""
    client = Minio(
        endpoint=config.endpoint,
        access_key=config.key,
        secret_key=config.secret,
        secure=True,
    )

    return client


def objstr_make_bucket(
    config: Box,
    bucket_name: str,
    client: Minio = None,
):
    """attempts to make a bucket if not already available"""

    # if client was not provided, build a client
    if not client:
        client = objstr_client(config=config)

    # make bucket if not exists
    if not client.bucket_exists(bucket_name):
        client.make_bucket(bucket_name)
        logging.info("Created bucket %s", bucket_name)
    else:
        logging.info("Bucket %s already exists, taking no actions.", bucket_name)


def objstr_fput(
    config: Box,
    bucket_name: str,
    object_name: str,
    file_path: str,
    client: Minio = None,
):
    """put file as object in object store"""

    # if client was not provided, build a client
    if not client:
        client = objstr_client(config=config)

    # upload file as object
    client.fput_object(
        bucket_name=bucket_name,
        object_name=object_name,
        file_path=file_path,
    )

    logging.info("Put file %s under %s as %s", file_path, bucket_name, object_name)


def objstr_fget(
    config: Box,
    bucket_name: str,
    object_name: str,
    file_path: str,
    client: Minio = None,
):
    """get object as file from object store"""

    # if client was not provided, build a client
    if not client:
        client = objstr_client(config=config)

    # get object as file
    client.fget_object(
        bucket_name=bucket_name,
        object_name=object_name,
        file_path=file_path,
    )

    logging.info(
        "Retrieved object %s under %s as file %s", object_name, bucket_name, file_path
    )
