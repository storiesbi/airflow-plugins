from datetime import datetime

import pytest
from airflow.models import TaskInstance
from airflow.operators.dummy_operator import DummyOperator
from airflow.utils.state import State
from airflow_plugins.operators import RunEvaluationOperator
from mock import Mock

DEFAULT_DATE = datetime(2017, 3, 28)


def test_execution_fails_on_failed_tasks():
    ti = TaskInstance(DummyOperator(task_id='test'), DEFAULT_DATE)
    ti.state = State.FAILED

    dag_run_mock = Mock()
    dag_run_mock.get_task_instances.return_value = [ti]

    op = RunEvaluationOperator(task_id='evaluation')

    with pytest.raises(RuntimeError) as e:
        op.execute({'dag_run': dag_run_mock})

    assert "Failed tasks instances detected" in str(e)


def test_execution_without_failed_tasks():
    dag_run_mock = Mock()
    dag_run_mock.get_task_instances.return_value = [
        TaskInstance(DummyOperator(task_id='test'), DEFAULT_DATE)
    ]

    op = RunEvaluationOperator(task_id='evaluation')
    op.execute({'dag_run': dag_run_mock})
