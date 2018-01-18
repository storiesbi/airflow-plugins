import logging
import re

import psycopg2
import psycopg2.extensions
from airflow.hooks.postgres_hook import PostgresHook as PostgresHookBase
from airflow.operators.postgres_operator import \
    PostgresOperator as PostgresOperatorBase
from airflow.utils.decorators import apply_defaults
from airflow_plugins.operators.base import \
    PostgresOperator as PostgresOperatorStatic


class PostgresHook(PostgresHookBase):

    """Tuned PostgreSQL hook which support
    running SQL like create database.
    Supports silent fail.
    """

    def __init__(self, database=None, fail_silently=False, *args, **kwargs):
        super(PostgresHook, self).__init__(*args, **kwargs)
        self.fail_silently = fail_silently
        self.schema = database

    def get_conn(self):
        conn = self.get_connection(self.postgres_conn_id)
        conn_args = dict(
            host=conn.host,
            user=conn.login,
            password=conn.password,
            dbname=self.schema or conn.schema,
            port=conn.port)
        # check for ssl parameters in conn.extra
        for arg_name, arg_val in conn.extra_dejson.items():
            if arg_name in ['sslmode', 'sslcert', 'sslkey',
                            'sslrootcert', 'sslcrl']:
                conn_args[arg_name] = arg_val
        psycopg2_conn = psycopg2.connect(**conn_args)
        if psycopg2_conn.server_version < 70400:
            self.supports_autocommit = True
        return psycopg2_conn

    def run(self, sql, autocommit=False, parameters=None):
        """
        Runs a command or a list of commands. Pass a list of sql
        statements to the sql parameter to get them to execute
        sequentially

        :param sql: the sql statement to be executed (str) or a list of
            sql statements to execute
        :type sql: str or list
        :param autocommit: What to set the connection's autocommit setting to
            before executing the query.
        :type autocommit: bool
        :param parameters: The parameters to render the SQL query with.
        :type parameters: mapping or iterable
        """
        conn = self.get_conn()
        if isinstance(sql, str):
            sql = [sql]

        self.set_autocommit(conn, autocommit)

        cur = conn.cursor()
        for s in sql:
            logging.info(s)
            if parameters is not None:
                cur.execute(s, parameters)
            else:
                if self.fail_silently:
                    try:
                        cur.execute(s)
                        conn.commit()
                    except Exception as e:
                        conn.rollback()
                        logging.exception(e)
                else:
                    cur.execute(s)
                    conn.commit()

        cur.close()
        conn.close()


class PostgresOperator(PostgresOperatorBase):

    """PostgreSQL operator which uses PostgresHook"""

    @apply_defaults
    def __init__(self, database=None, fail_silently=True, *args, **kwargs):
        super(PostgresOperator, self).__init__(*args, **kwargs)
        self.fail_silently = fail_silently
        self.schema = database

    def pre_execute(self, context):
        self.hook = PostgresHook(postgres_conn_id=self.postgres_conn_id,
                                 database=self.schema,
                                 fail_silently=self.fail_silently)

    def execute(self, context):
        logging.info('Executing: ' + str(self.sql))
        self.hook.run(self.sql, self.autocommit, parameters=self.parameters)


class CreateDatabase(PostgresOperatorStatic):

    """Operator which creates database in PostgreSQL."""

    _sql = [
        "CREATE DATABASE {{ params.database_name }};",  # keep create db at top
        "GRANT ALL PRIVILEGES ON DATABASE {{ params.database_name }} "
        "TO {{ params.user }};",  # set user in pre_execute if not in params
    ]

    def pre_execute(self, context):
        params = context['params']
        company = params.get('company')
        if company is not None:
            db_name = params['database_name']
            self.params['database_name'] = company.lower() + '_' + db_name

        hook = PostgresHook(postgres_conn_id=self.postgres_conn_id)
        conn = hook.get_connection(self.postgres_conn_id)
        if conn is not None:
            user = params.get('user', conn.login)
            if user == conn.login:
                self.params['user'] = user
            else:
                for item in reversed([
                    "CREATE USER {{ params.user }} "
                    "WITH PASSWORD '{{ params.password }}';",
                    "ALTER ROLE {{ params.user }} "
                    "SET client_encoding TO 'utf8';",
                    "ALTER ROLE {{ params.user }} "
                    "SET default_transaction_isolation TO 'read committed';",
                    "ALTER ROLE {{ params.user }} SET timezone TO 'UTC';",
                ]):
                    self._sql.insert(1, item)

        self.sql = self._sql
        context['ti'].render_templates()

    def execute(self, context):
        sqls = (self.sql[0:1], self.sql[1:])
        logging.info('Executing: ' + str(self.sql))

        self.hook = PostgresHook(postgres_conn_id=self.postgres_conn_id,
                                 fail_silently=True)  # fails if db exists
        self.hook.run(sqls[0], self.autocommit, parameters=self.parameters)
        self.hook = PostgresHook(postgres_conn_id=self.postgres_conn_id,
                                 fail_silently=False)  # should not fail
        if len(sqls[1]) > 0:
            self.hook.run(sqls[1], self.autocommit, parameters=self.parameters)


class DropDatabase(PostgresOperatorStatic):

    """Drop database operator."""

    sql = [
        "DROP DATABASE {{ params.company|lower }}_{{ params.database_name }};",
    ]

    def execute(self, context):
        logging.info('Executing: ' + str(self.sql))

        self.hook = PostgresHook(postgres_conn_id=self.postgres_conn_id,
                                 fail_silently=True)
        self.hook.run(self.sql, self.autocommit, parameters=self.parameters)


class ChangeDatabaseName(PostgresOperatorStatic):

    """Rename database in operator."""

    sql = [
        "ALTER DATABASE {{ params.company|lower }}_{{ params.database_name }} "
        "RENAME TO {{ params.company|lower }}_{{ params.database_name }}"
        "_{{ ts_nodash[:15] }};",
    ]

    def execute(self, context):
        logging.info('Executing: ' + str(self.sql))

        self.hook = PostgresHook(postgres_conn_id=self.postgres_conn_id,
                                 fail_silently=False)
        self.hook.run(self.sql, autocommit=True,
                      parameters=self.parameters)


class CreateTableWithColumns(PostgresOperator):

    """Create database with columns."""

    _sql = [
        "DROP TABLE IF EXISTS {{ params.table_name }};",
        "CREATE TABLE {{ params.table_name }} ({{ params.table_columns }});"
    ]

    @apply_defaults
    def __init__(self, *args, **kwargs):
        super(CreateTableWithColumns, self).__init__(sql=self._sql,
                                                     *args, **kwargs)

    @classmethod
    def _parse_extra_args(cls, args):
        parsed = []
        if not args:
            return parsed
        if not isinstance(args, list):
            args = args.strip().split()

        def add_stripped(*values):
            for val in values:
                if not val:
                    continue
                else:
                    while (len(val) > 1
                            and val[0] == val[-1]
                            and val[0] in ['"', "'"]):
                        val = val[1:-1]
                    if val:
                        parsed.append(val)

        for arg in args:
            if re.match(r'-[-\w]+=', arg):
                argsplit = arg.split('=')
                opt = argsplit[0]
                val = '='.join(argsplit[1:])
                add_stripped(opt, val)
            else:
                add_stripped(arg)

        return parsed

    @classmethod
    def _get_table_columns(cls, csv_file_path, extra=None):
        from io import StringIO
        from csvkit.utilities.csvcut import CSVCut
        output = StringIO()
        extra_args = cls._parse_extra_args(extra) if extra else []
        args = ['-n', *extra_args, csv_file_path]
        print(args)
        csvcut = CSVCut(args=args)
        csvcut.output_file = output
        csvcut.run()
        csv_columns = [
            col.split(': ')[1]
            for col in output.getvalue().splitlines()
        ]
        table_columns = [
            '"{}"'.format(col)
            if (col != col.lower() or ' ' in col) else col
            for col in csv_columns
        ]
        return table_columns

    def pre_execute(self, context):
        if context['params'].get('table_columns') is None:
            columns = self._get_table_columns(context['params']['csv_file'],
                                              context['params'].get('extra'))
            self.params['table_columns'] = ', '.join([
                '{} TEXT'.format(col) for col in columns
            ])
            self.sql = self._sql
            context['ti'].render_templates()
        super(CreateTableWithColumns, self).pre_execute(context)
