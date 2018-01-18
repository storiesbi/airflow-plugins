import re
from urllib.parse import urlparse

from airflow.models import BaseOperator
from airflow.operators.bash_operator import BashOperator as BashOperatorBase
from airflow.operators.postgres_operator import \
    PostgresOperator as PostgresOperatorBase
from airflow.utils.decorators import apply_defaults

from airflow_plugins import utils


class ExecutableOperator(BaseOperator):
    """
    Simple wrapper around command line executable programs with helper
    functions to add options, flags and arguments.
    """
    bash_command = ""

    def add_flag(self, flag_name):
        """Add boolean flag option used as enabled or disabled state"""
        self.bash_command += " {0}".format(flag_name)

    def add_option(self, option_name, value):
        """Add option to command"""
        if value is "" or value is None:
            return

        if isinstance(value, str) and '--' in value:
            options = " {0} {1}".format(option_name, value)
        else:
            options = ' {0} "{1}"'.format(option_name, value)
        options = re.sub('\s+', ' ', options)
        self.bash_command += options


class BashOperator(BashOperatorBase):

    """Bash Operator
    """

    bash_command = None

    @apply_defaults
    def __init__(self, bash_command=None, *args, **kwargs):
        super(BashOperator, self).__init__(
            bash_command=bash_command or self.bash_command, *args, **kwargs)


class PostgresOperator(PostgresOperatorBase):

    """Run SQL on Postgresql based systems.
    """

    sql = None

    @apply_defaults
    def __init__(self, sql=None, *args, **kwargs):
        super(PostgresOperator, self).__init__(
            sql=sql or self.sql, *args, **kwargs)


class FileOperator(BaseOperator):

    @staticmethod
    def _split_path(path):
        parsed = urlparse(path)
        scheme = parsed.scheme
        netloc = parsed.netloc if scheme else None
        path = parsed.path if scheme else path
        return (scheme, netloc, path)

    def _get_ftp_path(self, path):
        return self._split_path(path)[-1]

    def _get_s3_path(self, path):
        bucket, key = self._split_path(path)[1:]
        bucket = bucket or 'storiesbi-datapipeline'
        return (bucket, key)

    def pre_execute(self, context):
        params = context['params']
        for param in ['local_path', 'remote_path']:
            setattr(self, param, params.get(param))

        conn_id = None
        if hasattr(self, 'conn_id'):
            conn_id = self.conn_id

        if not conn_id:
            conn_params = ['conn_id', 'remote_connection']
            for conn_param in conn_params:
                conn_id = params.get(conn_param)
                if conn_id:
                    break

        if not conn_id:
            path_attrs = ['path', 'remote_path']
            for path_attr in path_attrs:
                if hasattr(self, path_attr):
                    path = getattr(self, path_attr)
                    if path:
                        engine, target = self._split_path(path)[:2]
                        if engine == 'ftp':
                            conn_id = target
                        elif engine == 's3':
                            conn_id = 's3.stories.bi'
                        break

        conn = utils.get_connection(conn_id)
        self.conn_id = conn_id
        self.conn = conn
