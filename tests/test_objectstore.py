""" Tests objectstore nuggets """

import logging
import tempfile

import pytest
import urllib3
from box import Box
from minio import Minio
from cupyopt.objectstore_tasks import (
    ObjstrClient,
    ObjstrFGet,
    ObjstrFPut,
    ObjstrGet,
    ObjstrMakeBucket,
    ObjstrPut,
)

logger = logging.getLogger(__name__)


@pytest.fixture(name="objstr_config")
def fixture_objstr_config():
    """fixture config for testing"""
    test_config = Box({"endpoint": "test", "key": "testkey", "secret": "testsecret"})

    return test_config


@pytest.fixture(name="objstr_client")
def fixture_objstr_client(objstr_config):
    """fixture client for testing"""
    client = ObjstrClient().run(
        config_box=objstr_config,
        http_client=urllib3.PoolManager(
            timeout=1,
            retries=urllib3.Retry(
                total=0,
            ),
        ),
    )

    return client


def test_client(objstr_client):
    """test client creation"""

    assert isinstance(objstr_client, Minio)


def test_make_bucket(objstr_client):
    """test make bucket"""
    with pytest.raises(urllib3.exceptions.MaxRetryError):
        ObjstrMakeBucket().run(
            client=objstr_client,
            bucket_name="bucket",
        )


def test_get(objstr_client):
    """test get"""
    with pytest.raises(urllib3.exceptions.MaxRetryError):
        ObjstrGet().run(
            client=objstr_client,
            bucket_name="bucket",
            object_name="object",
        )


def test_put(objstr_client):
    """test put"""
    with pytest.raises(AttributeError):
        ObjstrPut().run(
            client=objstr_client,
            bucket_name="bucket",
            object_name="object",
            data="some data",
        )

    with pytest.raises(urllib3.exceptions.MaxRetryError):
        with tempfile.NamedTemporaryFile() as temp_file:
            with open(temp_file.name, "r", encoding="utf-8") as open_data:
                ObjstrPut().run(
                    client=objstr_client,
                    bucket_name="bucket",
                    object_name="object",
                    data=open_data,
                )


def test_fput(objstr_client):
    """test fput"""
    with pytest.raises(urllib3.exceptions.MaxRetryError):
        with tempfile.NamedTemporaryFile() as temp_file:
            ObjstrFPut().run(
                client=objstr_client,
                bucket_name="bucket",
                object_name="object",
                file_path=temp_file.name,
            )


def test_fget(objstr_client):
    """test fget"""
    with pytest.raises(urllib3.exceptions.MaxRetryError):
        ObjstrFGet().run(
            client=objstr_client,
            bucket_name="bucket",
            object_name="object",
            file_path="temp.txt",
        )
