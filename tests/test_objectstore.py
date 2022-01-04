""" Tests objectstore nuggets """

import tempfile

import pytest
import urllib3
from box import Box
from minio import Minio
from cupyopt.nuggets import objectstore

test_config = Box({"endpoint": "test", "key": "testkey", "secret": "testsecret"})


def test_client():
    """test client creation"""
    client = objectstore.objstr_client(config=test_config)

    assert isinstance(client, Minio)


def test_make_bucket():
    """test make bucket"""
    with pytest.raises(urllib3.exceptions.MaxRetryError):
        objectstore.objstr_make_bucket(
            config=test_config,
            bucket_name="bucket",
        )


def test_fput():
    """test fput"""
    with pytest.raises(urllib3.exceptions.MaxRetryError):
        with tempfile.NamedTemporaryFile() as temp_file:
            objectstore.objstr_fput(
                config=test_config,
                bucket_name="bucket",
                object_name="object",
                file_path=temp_file.name,
            )


def test_fget():
    """test fget"""
    with pytest.raises(urllib3.exceptions.MaxRetryError):
        objectstore.objstr_fget(
            config=test_config,
            bucket_name="bucket",
            object_name="object",
            file_path="temp.txt",
        )
