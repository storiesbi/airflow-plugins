from os.path import dirname, join, realpath
from datetime import datetime
from airflow import DAG
from airflow.models import TaskInstance
from airflow_plugins.operators.clickhouse_operator import ClickHouseLoadCsvOperator  # noqa: 501
import dateutil

DAG_ID = 'test_clickhouse'


def test_load_csv():
    """
    Sample table created with:

    ```CREATE TABLE raw.test_clickhouse_load (
        id String,
        name Nullable(String),
        amount Nullable(UInt64),
        price Nullable(Float64),
        created_at Datetime,
        updated_at Datetime
    ) ENGINE = Memory
    ```
    """

    filename = 'sample_data_clickhouse.csv'
    filepath = join(dirname(realpath(__file__)), filename)
    schema = {
        'id': lambda x: str(x),
        'name': lambda x: str(x),
        'amount': lambda x: int(x),
        'price': lambda x: float(x),
        'created_at': lambda x: dateutil.parser.parse(x),
        'updated_at': lambda x: dateutil.parser.parse(x),
    }
    now_date = datetime.utcnow()
    task = ClickHouseLoadCsvOperator(**{
        'dag': DAG(**{
            'dag_id': DAG_ID,
            'start_date': now_date,
        }),
        'task_id': 'anytask',
        'clickhouse_conn_id': 'clickhouse_staging',
        'filepath': filepath,
        'database': 'raw',
        'table': 'test_clickhouse_load',
        'schema': schema,
        'header': True,
        'delimiter': ';',
        'quotechar': '"',
        'lineterminator': '\n',
        'quoting': 1,
    })
    ti = TaskInstance(task=task, execution_date=now_date)
    result = task.execute(ti.get_template_context())
    assert result is True
