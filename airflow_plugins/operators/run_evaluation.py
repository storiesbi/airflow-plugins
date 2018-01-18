from airflow.models import BaseOperator
from airflow.utils.state import State


class RunEvaluationOperator(BaseOperator):
    """Check the pipeline status."""

    def execute(self, context):
        if not context['dag_run']:
            return

        dag_run = context['dag_run']
        tis = dag_run.get_task_instances()
        failed_tis = []
        for ti in tis:
            if ti.state == State.FAILED:
                failed_tis.append("{}.{}".format(ti.dag_id, ti.task_id))

        if len(failed_tis) == 0:
            return

        tasks_ids = ",".join(failed_tis)
        raise RuntimeError(
            "Failed tasks instances detected - `{}`.".format(tasks_ids)
        )
