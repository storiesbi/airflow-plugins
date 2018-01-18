from datetime import datetime, timedelta
from time import sleep

import boto
import pytest
from airflow.exceptions import AirflowException
from boto.s3.key import Key
from mock import Mock
from moto import mock_s3

from airflow_plugins.operators import FileSensor
from airflow_plugins.utils import get_or_update_conn

ctx = {'params': {}, 'ti': Mock(start_date=datetime.now())}


def test_files_sensor_fails_on_not_existing_connection():
    file_sensor = FileSensor(
        task_id="check_new_file",
        path="foo",
        conn_id="baz",
    )

    file_sensor.pre_execute(ctx)
    with pytest.raises(AirflowException) as e:
        file_sensor.poke(ctx)

    assert "Connection not found: `baz`" in str(e)


def test_files_sensor_fail_on_unsupported_connection():
    get_or_update_conn("baz_oracle", conn_type="oracle")

    file_sensor = FileSensor(
        task_id="check_new_file",
        path="foo",
        conn_id="baz_oracle",
    )

    file_sensor.pre_execute(ctx)
    with pytest.raises(NotImplementedError) as e:
        file_sensor.poke(ctx)

    assert "Unsupported engine: `oracle`" in str(e)


@mock_s3
def test_files_on_s3():
    conn = boto.connect_s3()
    bucket = conn.create_bucket('storiesbi-datapipeline')
    get_or_update_conn("s3.stories.bi", conn_type="s3")

    file_sensor = FileSensor(
        task_id="check_new_file",
        path="foo",
        conn_id="s3.stories.bi",
        modified="anytime"
    )

    file_sensor.pre_execute(ctx)

    assert not file_sensor.poke(ctx)

    k = Key(bucket)
    k.key = "foo"
    k.set_contents_from_string("bar")

    assert file_sensor.poke(ctx)


@mock_s3
def test_files_on_s3_modified_after():
    conn = boto.connect_s3()
    bucket = conn.create_bucket('storiesbi-datapipeline')

    k = Key(bucket)
    k.key = "foo"
    k.set_contents_from_string("bar")

    get_or_update_conn("s3.stories.bi", conn_type="s3")

    file_sensor = FileSensor(
        task_id="check_new_file",
        path="foo",
        conn_id="s3.stories.bi",
        modified=datetime.now()
    )

    file_sensor.pre_execute(ctx)

    assert not file_sensor.poke(ctx)

    # Hacky hacky!
    sleep(1)
    key = bucket.get_key("foo")
    key.set_contents_from_string("baz")

    assert file_sensor.poke(ctx)


@mock_s3
def test_files_on_s3_from_custom_bucket_defined_in_path():
    conn = boto.connect_s3()
    bucket = conn.create_bucket('testing')
    k = Key(bucket)
    k.key = "foo"
    k.set_contents_from_string("baz")

    get_or_update_conn("s3.stories.bi", conn_type="s3")
    yesterday = datetime.now() - timedelta(1)

    file_sensor = FileSensor(
        task_id="check_new_file",
        path="s3://testing/foo",
        conn_id="s3.stories.bi",
        modified=yesterday
    )

    file_sensor.pre_execute(ctx)

    assert file_sensor.poke(ctx)
