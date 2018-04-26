from datetime import datetime, timedelta
from time import sleep

import boto3
import pytest
from airflow.exceptions import AirflowException
# from boto3.s3.key import Key
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
    conn = boto3.resource('s3')
    conn.create_bucket(Bucket='storiesbi-datapipeline')

    get_or_update_conn("s3.stories.bi", conn_type="s3")

    file_sensor = FileSensor(
        task_id="check_new_file",
        path="ss://storiesbi-datapipeline/foo",
        conn_id="s3.stories.bi",
        modified="anytime"
    )

    file_sensor.pre_execute(ctx)

    assert not file_sensor.poke(ctx)

    conn.Object('storiesbi-datapipeline', 'foo').put(Body="bar")

    assert file_sensor.poke(ctx)


@mock_s3
def test_files_on_s3_modified_after():
    conn = boto3.resource('s3')
    conn.create_bucket(Bucket='storiesbi-datapipeline')
    conn.Object('storiesbi-datapipeline', 'foo').put(Body="bar")

    get_or_update_conn("s3.stories.bi", conn_type="s3")

    file_sensor = FileSensor(
        task_id="check_new_file",
        path="s3://storiesbi-datapipeline/foo",
        conn_id="s3.stories.bi",
        modified=datetime.now()
    )

    file_sensor.pre_execute(ctx)

    assert not file_sensor.poke(ctx)

    # Hacky hacky!
    sleep(1)
    conn.Object('storiesbi-datapipeline', 'foo').put(Body="baz")

    assert file_sensor.poke(ctx)


@mock_s3
def test_files_on_s3_from_custom_bucket_defined_in_path():
    conn = boto3.resource('s3')
    conn.create_bucket(Bucket='testing')
    conn.Object('testing', 'foo').put(Body="baz")

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


@mock_s3
def test_operator_notification():
    conn = boto3.resource('s3')
    conn.create_bucket(Bucket='testing')
    conn.Object('testing', 'foo').put(Body="baz")

    get_or_update_conn("s3.stories.bi", conn_type="s3")
    yesterday = datetime.now() - timedelta(1)

    file_sensor = FileSensor(
        task_id="check_new_file",
        path="s3://testing/foo",
        conn_id="s3.stories.bi",
        modified=yesterday
    )

    file_sensor._send_notification(ctx, success=False)
