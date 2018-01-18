from airflow import AirflowException
from airflow.utils.state import State


class ShutdownOnDependencyMissOperator:
    dependant_tasks = []

    def set_dependant_tasks(self, dependant_tasks=None):
        if not dependant_tasks:
            dependant_tasks = []

        self.dependant_tasks = dependant_tasks

    def pre_execute(self, context):
        failed = [
            State.SHUTDOWN,
            State.FAILED,
            State.SKIPPED,
            State.UPSTREAM_FAILED,
        ]

        for task in self.dependant_tasks:
            ti = context['dag_run'].get_task_instance(task)
            if not ti or ti.state in failed:
                raise AirflowException("Task `{}` failed, skipping...".format(
                    task
                ))
