import logging
import time

from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.utils.state import State


class DeferOperator(BaseOperator):

    """Check pipeline."""

    @apply_defaults
    def __init__(self, check_delay=5.0, *args, **kwargs):
        super(DeferOperator, self).__init__(*args, **kwargs)
        self.check_delay = check_delay
        self.deferred_task_ids = []

    def _tasks_finished(self, dag_run):
        for ti in dag_run.get_task_instances():
            if ti.task_id in self.deferred_task_ids:
                continue

            if ti.state in State.unfinished():
                logging.info("Deferred tasks are not yet executable. Found "
                             "unfinished task `{}`.".format(ti.task_id))
                return False

        return True

    def pre_execute(self, context):
        self.deferred_task_ids.append(context['task'].task_id)
        for task in context['task'].get_flat_relatives():
            self.deferred_task_ids.append(task.task_id)

    def execute(self, context):
        while not self._tasks_finished(context['dag_run']):
            logging.debug("Next check in {} s.".format(self.check_delay))
            time.sleep(self.check_delay)

        logging.info("Start executing deferred tasks: `{}`."
                     .format(", ".join(self.deferred_task_ids)))
