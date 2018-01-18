import logging
import pickle
from datetime import datetime, timedelta

from airflow.models import DagModel, TaskInstance
from airflow.operators.sensors import BaseSensorOperator
from airflow.settings import Session
from airflow.utils.decorators import apply_defaults
from airflow.utils.state import State

from airflow_plugins.operators.slack.notifications import send_notification


class TaskRuntimeSensor(BaseSensorOperator):

    """
    Checks whether particular tasks are still running
    after a period of time and notify about them if so

    :param notify_after: Start sending notifications after given number
        of seconds (of runtime)
    :type notify_after: int (or timedelta)
    :param notify_delta: Time interval between successive notifications
        in seconds, defaults to one hour (60*60 seconds)
    :type notify_delta: int (or timedelta)
    :param start_wait: Wait at start for at least given number of seconds
        for tasks to be registered (set if this op runs continuously)
    :type start_wait: int (or timedelta)
    :param dag_ids: List of dag_ids determining target task instances,
        can be set as a mask (e.g. "kiwi_master" for all kiwi master dags)
    :type dag_ids: list
    :param task_ids: List of task_ids determining target task instances
    :type task_ids: list
    :param operator_ids: List of operators determining target task instances
    :type operator_ids: list
    :param include_subdags: Whether to include subdags of target dags (dag_ids)
        (i.e. "kiwi_master" to also match "kiwi_master.storyteller" tasks),
        default True (always True if dag_ids not set)
    :type include_subdags: bool
    :param check_execution_time: Whether to check task instance execution time,
        or wall clock time (time elapsed from midnight), default True
    :type check_execution_time: bool
    """

    @apply_defaults
    def __init__(
            self,
            notify_after,
            notify_delta=60*60,
            start_wait=0,
            dag_ids=None,
            task_ids=None,
            operator_ids=None,
            include_subdags=True,
            check_execution_time=True,
            *args, **kwargs):

        super(TaskRuntimeSensor, self).__init__(*args, **kwargs)

        if dag_ids is None and task_ids is None and operator_ids is None:
            raise ValueError(
                'Provide at least one of `dag_ids`, `task_ids`, `operator_ids`'
                ' to determine the task instances to check.')

        self.include_subdags = include_subdags
        self.check_execution_time = check_execution_time
        self._init_target_variables(dag_ids=dag_ids,
                                    task_ids=task_ids,
                                    operator_ids=operator_ids)
        self._init_notification_variables(notify_after=notify_after,
                                          notify_delta=notify_delta,
                                          start_wait=start_wait)

    def _init_target_variables(self, **kwargs):
        for key, val in kwargs.items():
            if val is not None:
                val = [_id.strip() for _id in val.split(',') if _id.strip()]
            setattr(self, key, val)

    def _init_notification_variables(self, **kwargs):
        self.last_notifications = {}  # ti-specific last notification time
        for key, val in kwargs.items():
            if isinstance(val, int):
                val = timedelta(seconds=val)
            setattr(self, key, val)

    def _get_target_dags(self):
        session = Session()
        active_dags = session.query(DagModel.dag_id).filter(
            DagModel.is_paused.is_(False)).all()
        if self.dag_ids is None:
            target_dags = active_dags  # subdags always included
        else:
            target_dags = [
                dag_id for dag_id in active_dags
                if True in [
                    dag_id.startswith(dag_id_mask)
                    for dag_id_mask in self.dag_ids
                    if (self.include_subdags
                        or dag_id.count('.') == dag_id_mask.count('.'))
                ]
            ]
        return target_dags

    @staticmethod
    def _get_task_instance(key):
        TI = TaskInstance
        session = Session()
        # filter via key should uniquely indentify the instance
        tis = session.query(TI).filter(TI.dag_id == key[0],
                                       TI.task_id == key[1],
                                       TI.execution_date == key[2])
        return tis.first()  # returns None if no such TI found

    def _send_notification(self, ti, ti_key, finished=False):
        title = ti.task_id.upper()
        text = 'DAG: {}'.format(ti.dag_id)
        color = 'warning'

        if finished:
            title += ' finally finished (as {})'.format(ti.state)
            if ti.state == State.SUCCESS:
                title += ' :white_check_mark:'
                color = 'good'
        else:
            runtime = self.last_notifications[ti_key]
            title += ' is still running :redsiren:'
            text = 'Still not finished after {}'.format(
                str(runtime).split('.')[0]) + '\n' + text

        send_notification(ti.get_dagrun(), text, title, color)

    def pre_execute(self, context):
        self.save_path = '/tmp/{}.pkl'.format(
            '.'.join([self.__class__.__name__,
                      context['ti'].dag_id,
                      context['ti'].task_id]))

        try:
            with open(self.save_path, mode='rb') as f:
                save = pickle.load(f)
                if datetime.now() - save['timestamp'] < timedelta(hours=6):
                    self.last_notifications = save['data']
        except FileNotFoundError as e:
            logging.warning('Unable to load previous state: {}'.format(e))

        for target_name, target in [
            ('DAG', self.dag_ids),
            ('Task', self.task_ids),
            ('Operator', self.operator_ids),
        ]:
            logging.info('Poking for {}s: {}'.format(
                target_name,
                '--all--' if target is None else ', '.join(target)
            ))

        logging.info('Start notifying after {}, then periodically after {}'
                     .format(self.notify_after, self.notify_delta))

    def post_execute(self, context):
        save = {
            'timestamp': datetime.now(),
            'data': self.last_notifications,
        }
        try:
            with open(self.save_path, mode='wb') as f:
                pickle.dump(save, f)
        except Exception as e:
            logging.warning('Unable to save current state: {}'.format(e))

    def poke(self, context):
        TI = TaskInstance
        session = Session()
        tis = session.query(TI)

        tis = tis.filter(TI.state.in_(State.unfinished()))
        tis = tis.filter(TI.dag_id.in_(self._get_target_dags()))
        if self.task_ids:
            tis = tis.filter(TI.task_id.in_(self.task_ids))
        if self.operator_ids:
            # tis = tis.filter(TI.operator.in_(self.operator_ids))
            pass  # operator attribute might be None

        tis = tis.all()
        tis = [ti for ti in tis if ti.key != context['ti'].key]  # exclude self
        tis = [ti for ti in tis if (ti.operator is None
                                    or self.operator_ids is None
                                    or ti.operator in self.operator_ids)]

        if len(tis) == 0 and len(self.last_notifications) == 0:
            return datetime.now() >= context['ti'].start_date + self.start_wait

        now = datetime.now()
        start_midnight = datetime.combine(
            context['ti'].start_date, datetime.min.time())

        ti_keys = [(ti.dag_id, ti.task_id, ti.execution_date) for ti in tis]
        for ti, ti_key in zip(tis, ti_keys):
            if self.check_execution_time:
                start_date = ti.start_date
            else:
                start_date = start_midnight

            runtime = now - start_date
            if runtime >= self.notify_after:
                last_notification = self.last_notifications.get(ti_key)
                if (last_notification is None or
                        runtime >= last_notification + self.notify_delta):
                    self.last_notifications[ti_key] = runtime
                    self._send_notification(ti, ti_key, finished=False)

        # tis previously notified about but not found anymore -- finished
        ti_keys_to_delete = set(self.last_notifications) - set(ti_keys)
        for ti_key in ti_keys_to_delete:
            ti = self._get_task_instance(ti_key)
            if ti is not None:  # could be deleted from db (deleted via UI)
                self._send_notification(ti, ti_key, finished=True)
            del self.last_notifications[ti_key]

        # return len(self.last_notifications) == 0
        return True  # schedule regularly, always exit
