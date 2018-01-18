import logging
import os

from airflow_plugins.operators import BashOperator


class CSVLook(BashOperator):

    """Get stats of the CSV file"""

    bash_command = """
    csvlook {{ params.extra }} {{ params.path }}
    """


class CSVSQL(BashOperator):

    """Use csvsql tool for migration CSV to SQL.
    For more parameters check csvsql."""

    bash_command = """
    csvsql {{ params.extra }} {{ params.path }}
    """


class CSVtoDB(BashOperator):

    """Use csvsql tool for migration csv to SQL database.
    For more parameters check csvsql."""

    bash_command = """
    csvsql {{ params.extra }} \
        {%- if params.db %} --db="{{ params.db }}/{{ params.company|lower }} \
        {%- if params.company %}_{%- endif %}{{ params.database_name }}" \
        --no-inference -y 200 --insert --tables {{ params.get("table_name", "import") }} \
        {%- endif %} {{ params.local_path }}
    """  # noqa


class DBtoCSV(BashOperator):

    bash_command = """
    sql2csv {{ params.extra }} --query "{{ params.query }}" \
        {%- if params.db %} --db="{{ params.db }}/{{ params.company|lower }} \
        {%- if params.company %}_{%- endif %}{{ params.database_name }}" \
        {%- endif %} > {{ params.output_path_temp }}
    """  # noqa


class CSVStats(BashOperator):

    """Get stats of the CSV file
    Use csvstat.
    """

    bash_command = """
    csvstat {{ params.extra }} {{ params.path }}
    """


class SplitCSVtoDB(CSVtoDB):

    """Split CSV and upload to DB.
    """

    @staticmethod
    def _split_file(filepath, n):
        if n <= 1:
            return

        files = [open('{}.{}'.format(filepath, i), mode='w') for i in range(n)]

        with open(filepath, mode='r') as f:
            line = f.readline()
            for file in files:
                # header line
                file.write(line)

            i = 0
            line = f.readline()
            while line:
                files[i].write(line)
                line = f.readline()
                i = (i + 1) % n

        for file in files:
            file.close()

    @staticmethod
    def _determine_splits(filepath):
        size = os.stat(filepath).st_size
        # splits as hundreds of megabytes
        splits = size // (100 * 1000 * 1000) + 1
        logging.info('File size: {} bytes ==> {} splits'.format(
            size, splits if splits > 1 else 'no'))
        return splits

    def pre_execute(self, context):
        filepath = context['params']['local_path']
        self._splits = self._determine_splits(filepath)
        try:
            self._split_file(filepath, self._splits)
        except Exception as e:
            self._splits = 0
            logging.warning('Splitting the input file failed: {}'.format(e))
            logging.info('Trying to load the whole file.')
        if self._splits > 1:
            self.bash_command = 'for i in $(seq 0 {}); do {}.$i; done'.format(
                self._splits - 1, self.bash_command.strip())

    def post_execute(self, context):
        filepath = context['params']['local_path']
        if self._splits > 1:
            for i in range(self._splits):
                file = '{}.{}'.format(filepath, i)
                try:
                    os.remove(file)
                except Exception as e:
                    # it's ok, these are just helper files
                    logging.warning('Unable to delete file'
                                    '{}: {}'.format(file, e))
