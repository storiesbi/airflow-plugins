from airflow.exceptions import AirflowException
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow_plugins.hooks.clickhouse_hook import ClickHouseHook
import csv


class ClickHouseLoadCsvOperator(BaseOperator):
    template_fields = ('filepath', 'database', 'table',)

    @apply_defaults
    def __init__(self,
                 clickhouse_conn_id,
                 filepath,
                 database,
                 table,
                 schema,
                 header=True,
                 delimiter=',',
                 quotechar='"',
                 lineterminator='\n',
                 quoting=csv.QUOTE_MINIMAL,
                 *args,
                 **kwargs):
        super(ClickHouseLoadCsvOperator, self).__init__(*args, **kwargs)
        self.clickhouse_conn_id = clickhouse_conn_id
        self.filepath = filepath
        self.database = database
        self.table = table
        self.schema = schema
        self.header = header
        self.delimiter = delimiter
        self.quotechar = quotechar
        self.lineterminator = lineterminator
        self.quoting = quoting

    def _parse_row(self, row):
        row_parsed = []

        for column, method in self.schema.items():
            row_parsed.append(method(row[column]))

        return row_parsed

    def execute(self, context):
        hook = ClickHouseHook(self.clickhouse_conn_id)
        values = []
        columns = self.schema.keys()

        with open(self.filepath, 'r') as f:
            reader = csv.DictReader(f, **{
                'fieldnames': columns,
                'delimiter': self.delimiter,
                'quotechar': self.quotechar,
                'lineterminator': self.lineterminator,
                'quoting': self.quoting,
            })
            if self.header:
                next(reader)

            for row in reader:
                values.append(self._parse_row(row))

        if not values:
            raise AirflowException("Empty file '{}'".format(self.filepath))

        result = hook.insert_into(self.database, self.table, values)
        return result
