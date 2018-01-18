import logging
from datetime import datetime

import pytest
from airflow import AirflowException
from airflow.models import TaskInstance
from airflow.operators.dummy_operator import DummyOperator
from airflow.utils.state import State
from airflow_plugins.operators import SaveStatusOperator
from airflow_plugins.xcom import Xcom
from mock import Mock, mock
from testfixtures import LogCapture


def _get_operator(responses):
    default_args = {'username': "admin", 'password': "admin",
                    'company': "Stories", 'environment': "stg"}

    op = SaveStatusOperator(task_id="test", default_args=default_args,
                            rid_xcom=Xcom("rid"), host_xcom=Xcom("hostname"))
    op.RETRY_DELAY = 0.1

    client_mock = Mock()
    client_mock.get_save_status_by_rid.side_effect = responses

    op._SaveStatusOperator__client = client_mock

    context = {
        'ti': TaskInstance(op, datetime.now())
    }

    return op, context


def _execute_operator(op, context):
    with mock.patch("airflow.models.functools.partial") as pull_func_mock:
        func_mock = Mock()
        func_mock.side_effect = [
            "2017-05-09-11-15-03-191775",
            "http://localhost:5000"
        ]
        pull_func_mock.return_value = func_mock

        op.execute(context)


@pytest.mark.parametrize("responses,expected_err", [
    # wrong response
    ([None], "Unsupported response on status_by_rid call."),

    # error in response
    (
        [{
            'data': {
                'locked': False,
                'result': {
                    'success': False,
                    'error': 'Foobar',
                }
            }
        }],
        ""
    ),
])
def test_save_stories_fails(responses, expected_err):
    op, context = _get_operator(responses)

    with pytest.raises(AirflowException) as err:
        _execute_operator(op, context)

    assert expected_err in str(err)


@pytest.mark.parametrize("responses, expected_logs", [
    # Save wait during external lock
    (
        [
            {
                'data': {
                    'active': True,
                    'locked': False,
                    'result': {}
                }
            },
            {
                'data': {
                    'active': False,
                    'locked': False,
                    'result': {
                        'success': True,
                    },
                }
            }
        ],
        [
            ('root', 'INFO', "Saving has not started yet, there is another"
                             "running pipeline for same run."),
            ('root', 'INFO', "Saving status for RID "
                             "`2017-05-09-11-15-03-191775`: Done."),
        ]
    ),

    # Success based on success property in new response version
    (
        [
            {
                'data': {
                    'active': True,
                    'locked': True,
                    'result': {
                        'success': False,
                        'tasks': 5,
                        'tasks_done': 1,
                    },
                }
            },
            {
                'data': {
                    'active': False,
                    'locked': False,
                    'result': {
                        'success': True,
                        'tasks': 5,
                        'tasks_done': 5,
                    },
                }
            }
        ],
        [
            ('root', 'INFO', "Saving status for RID "
                             "`2017-05-09-11-15-03-191775`: In progress 1/5."),
            ('root', 'INFO', "Saving status for RID "
                             "`2017-05-09-11-15-03-191775`: Done."),
        ]
    ),
])
def test_save_stories(responses, expected_logs):
    op, context = _get_operator(responses)

    with LogCapture(level=logging.INFO) as logs:
        _execute_operator(op, context)

    logs.check(*expected_logs)


@pytest.mark.parametrize("task_instance,raises", [
    # no storyteller TI
    (None, True),
    # failed storyteller TI
    (TaskInstance(DummyOperator(task_id="storyteller"), datetime.now(),
                  State.FAILED), True),
    # successful storyteller TI
    (TaskInstance(DummyOperator(task_id="storyteller"), datetime.now(),
                  State.SUCCESS), False),
])
def test_skip_when_storyteller_failed(task_instance, raises):
    op, context = _get_operator({})
    op.set_dependant_tasks(["storyteller"])

    dag_run = Mock()
    dag_run.get_task_instance.return_value = task_instance

    context['dag_run'] = dag_run

    if raises:
        with pytest.raises(AirflowException):
            op.pre_execute(context)
    else:
        op.pre_execute(context)
