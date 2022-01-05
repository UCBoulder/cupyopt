""" Tests objectstore nuggets """

import logging
import tempfile

import pytest
import urllib3
from box import Box
from cupyopt.objectstore_tasks import (
    Objstr_client,
    Objstr_fput,
    Objstr_make_bucket,
    Objstr_fget,
)
from minio import Minio

logger = logging.getLogger(__name__)
test_config = Box({"endpoint": "test", "key": "testkey", "secret": "testsecret"})


@pytest.fixture
def objstr_client():
    """fixture client for testing"""
    client = Objstr_client().run(
        config_box=test_config,
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
        Objstr_make_bucket().run(
            client=objstr_client,
            bucket_name="bucket",
        )


def test_fput(objstr_client):
    """test fput"""
    with pytest.raises(urllib3.exceptions.MaxRetryError):
        with tempfile.NamedTemporaryFile() as temp_file:
            Objstr_fput().run(
                client=objstr_client,
                bucket_name="bucket",
                object_name="object",
                file_path=temp_file.name,
            )


def test_fget(objstr_client):
    """test fget"""
    with pytest.raises(urllib3.exceptions.MaxRetryError):
        Objstr_fget().run(
            client=objstr_client,
            bucket_name="bucket",
            object_name="object",
            file_path="temp.txt",
        )
