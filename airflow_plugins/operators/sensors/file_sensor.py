import logging
import os.path
import time
from datetime import datetime, timedelta

from airflow.exceptions import (
    AirflowException,
    AirflowSensorTimeout,
    AirflowSkipException
)
from airflow.hooks.S3_hook import S3Hook
from airflow.operators.sensors import BaseSensorOperator
from airflow.utils.decorators import apply_defaults
from pytz import timezone

from airflow_plugins.hooks import FTPHook
from airflow_plugins.operators import FileOperator
from airflow_plugins.operators.slack.notifications import send_notification


class FileSensor(BaseSensorOperator, FileOperator):

    """Check file presence on hook"""

    @apply_defaults
    def __init__(
            self, path,
            modified=None,
            notify_after=8*60*60,
            notify_delta=1*60*60,
            conn_id=None,
            *args, **kwargs):
        super(FileSensor, self).__init__(*args, **kwargs)

        self.path = path
        self.modified = modified
        self._init_notification_variables(notify_after=notify_after,
                                          notify_delta=notify_delta)
        self.conn_id = conn_id

    def _init_notification_variables(self, **kwargs):
        self.last_notification = None
        for key, val in kwargs.items():
            if isinstance(val, int):
                val = timedelta(seconds=val)
            setattr(self, key, val)

    def _send_notification(self, context, success=False):
        if self.notify_after is None:
            return

        ti = context['ti']
        title = ti.task_id.upper()
        text_lines = [
            'Path: {}'.format(self.path),
            'DAG: {}'.format(ti.dag_id),
        ]
        color = 'warning'

        if success:
            color = 'good'
            title += ' finally' * (self.last_notification is not None)
            title += ' succeeded :white_check_mark:'
            text = '\n'.join(text_lines)
            logging.info('Sending notification about exit.')
            send_notification(ti.get_dagrun(), text, title, color)
            return

        runtime = datetime.now() - ti.start_date
        if runtime >= self.notify_after:
            if (self.last_notification is None or
                    runtime >= self.last_notification + self.notify_delta):
                title += ' is still waiting :redsiren:'
                runtime_str = str(runtime).split('.')[0]
                text = 'Still not finished after {}'.format(runtime_str)
                text = '\n'.join([text, *text_lines])
                logging.info('Sending notification about runtime.')
                send_notification(ti.get_dagrun(), text, title, color)
                self.last_notification = runtime

    def pre_execute(self, context):
        FileOperator.pre_execute(self, context)

        if self.modified is None:
            self.modified = context['ti'].start_date

        def floor_datetime(dt, precision):
            dt_items = ('microsecond', 'second', 'minute', 'hour', 'day')
            to_replace = dt_items[:dt_items.index(precision)]
            replaced = {item: 0 for item in to_replace}
            return dt.replace(**replaced)

        dt = context['ti'].start_date
        dt_day = floor_datetime(dt, 'day')

        if isinstance(self.modified, str):
            # H / D / W / M / A <==> start of:
            # hour / day / week / month / anytime
            modkey = self.modified[0].upper()
            for precision in ['hour', 'day']:
                if modkey == precision[0].upper():
                    self.modified = floor_datetime(dt, precision)
                    break
            else:
                if modkey == 'W':  # week start (Monday)
                    self.modified = dt_day - timedelta(days=dt_day.weekday())
                elif modkey == 'M':  # month start (1st day)
                    self.modified = dt_day - timedelta(days=(dt_day.day - 1))
                elif modkey == 'A':  # anytime -- exists
                    self.modified = None
                else:
                    raise AirflowException('Unable to devise modified time: {}'
                                           ' (supported: H / D / W / M / A)'
                                           .format(self.modified))

        elif isinstance(self.modified, int):
            modkey = self.modified
            if modkey < 0:
                # subtract given number of days
                daydiff = abs(modkey)
                self.modified = dt_day - timedelta(days=daydiff)
            else:  # modkey >= 0
                # 1-7 <==> last Monday-Sunday (goes at most 6 days back)
                if not (1 <= self.modified <= 7):
                    raise AirflowException('Unable to devise modified time: {}'
                                           ' (a weekday number expected, 1-7)'
                                           .format(self.modified))

                weekday = dt.weekday() + 1  # datetime weekdays 0-6
                daydiff = (weekday - modkey) % 7
                self.modified = dt_day - timedelta(days=daydiff)

    def execute(self, context):
        started_at = datetime.now()
        while True:
            poke_result = self.poke(context)
            if poke_result:
                break
            if (datetime.now() - started_at).total_seconds() > self.timeout:
                timeout_msg = 'Snap. Time is OUT.'
                if self.soft_fail:
                    raise AirflowSkipException(timeout_msg)
                else:
                    raise AirflowSensorTimeout(timeout_msg)
            else:
                self._send_notification(context, success=False)
                time.sleep(self.poke_interval)
        if self.last_notification is not None:
            # notify about success in case of previous warnings
            self._send_notification(context, success=True)
        logging.info('Success criteria met. Exiting.')
        return poke_result

    def poke(self, context):
        logging.info(
            'Poking for file: {} in {}'.format(self.path, self.conn_id))

        if not self.conn:
            raise AirflowException(
                "Connection not found: `{}`".format(self.conn_id))

        if self.conn.conn_type not in ["ftp", "s3"]:
            raise NotImplementedError(
                "Unsupported engine: `{}`".format(self.conn.conn_type))

        if self.conn.conn_type == "ftp":
            hook = FTPHook(self.conn_id)
            try:
                path = self._get_ftp_path(self.path)
                last_modified = hook.get_mod_time(path)
            except Exception as e:
                msg = ('Error getting file modification time: {} '
                       '(The file most likely does not exist)'
                       .format(e))
                if self.modified:
                    # looking for a new version of the file
                    raise AirflowException(msg)
                else:
                    # waiting for the file to appear
                    logging.warning(msg)
                    return False

        elif self.conn.conn_type == "s3":
            hook = S3Hook(self.conn_id)
            bucket, key = self._get_s3_path(self.path)
            fileobj = hook.get_bucket(bucket).get_key(key)

            if not fileobj:
                msg = 'The file does not exist'
                if self.modified:
                    # looking for a new version of the file
                    raise AirflowException(msg)
                else:
                    # waiting for the file to appear
                    logging.info(msg)
                    return False

            def get_last_modified(fileobj):
                timestamp = fileobj.last_modified
                tformat = '%a, %d %b %Y %H:%M:%S %Z'
                dt = datetime.strptime(timestamp, tformat)
                t = time.strptime(timestamp, tformat)

                try:
                    tz = timezone(t.tm_zone)
                except AttributeError:  # tm_zone not set on t
                    return dt
                else:
                    dt_local = dt.replace(tzinfo=tz).astimezone()
                    return dt_local.replace(tzinfo=None)

            last_modified = get_last_modified(fileobj)

        if not self.modified:
            logging.info('File found, last modified: {}'
                         .format(last_modified.isoformat()))
            return last_modified

        logging.info(
            "File last modified: {} (checking for {})".format(
                last_modified.isoformat(),
                self.modified.isoformat()))

        if last_modified > self.modified:
            return last_modified
        else:
            return False


class FTPDirSensor(FileSensor):

    def pre_execute(self, context):
        super(FTPDirSensor, self).pre_execute(context)
        self.dirpath = self.path

    def poke(self, context):
        hook = FTPHook(self.conn_id)
        dirpath = self._get_ftp_path(self.dirpath)
        files = hook.list_directory(dirpath)
        if len(files) == 0:
            logging.info('Directory {} is empty'.format(self.dirpath))
            return False
        else:
            filepaths = [os.path.join(self.dirpath, f) for f in files]
            filemodts = {
                f: hook.get_mod_time(self._get_ftp_path(f))
                for f in filepaths
            }
            self.path = sorted(filepaths, key=lambda f: filemodts[f])[-1]
            return super(FTPDirSensor, self).poke(context)

    def post_execute(self, context):
        super(FTPDirSensor, self).post_execute(context)
        context['ti'].xcom_push(key='remote_path', value=self.path)
