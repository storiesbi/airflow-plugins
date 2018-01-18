class Xcom(object):
    """XCOM DAO used for xcom pushes and pulls"""
    def __init__(self, tasks_ids, dag_id=None):
        self.tasks_ids = tasks_ids
        self.dag_id = dag_id
