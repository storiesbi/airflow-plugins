import logging

from airflow.exceptions import AirflowException
from airflow.hooks.S3_hook import S3Hook
from airflow_plugins.hooks import FTPHook
from airflow_plugins.operators import FileOperator


class DynamicTargetFile(FileOperator):

    """Dynamic target file operator"""

    def pre_execute(self, context):
        params = context['params']
        for target in ['local_path', 'remote_path']:
            value = context['ti'].xcom_pull(task_ids=None, key=target)
            if value is not None:
                # update context params for base pre_execute
                params[target] = value
        super(DynamicTargetFile, self).pre_execute(context)


class DownloadFile(FileOperator):
    """Download file operator."""

    def execute(self, context):
        logging.info(
            "Downloading %s to %s" % (self.remote_path, self.local_path))

        if self.conn and self.conn.conn_type == "ftp":
            hook = FTPHook(self.conn_id)
            path = self._get_ftp_path(self.remote_path)
            hook.retrieve_file(path, self.local_path)

        elif self.conn and self.conn.conn_type == "s3":
            hook = S3Hook(self.conn_id)
            bucket, key = self._get_s3_path(self.remote_path)
            fileobj = hook.get_bucket(bucket).get_key(key)
            fileobj.get_contents_to_filename(self.local_path)

        else:
            raise AirflowException('Connection: {}'.format(self.conn_id))


class DynamicDownloadFile(DownloadFile, DynamicTargetFile):
    """Dynamic download file operator."""
    pass


class UploadFile(FileOperator):
    """Upload file operator."""

    def execute(self, context):
        logging.info(
            "Uploading %s to %s" % (self.local_path, self.remote_path))

        if self.conn and self.conn.conn_type == "ftp":
            hook = FTPHook(self.conn_id)
            path = self._get_ftp_path(self.remote_path)
            hook.store_file(path, self.local_path)

        elif self.conn and self.conn.conn_type == "s3":
            hook = S3Hook(self.conn_id)
            bucket, key = self._get_s3_path(self.remote_path)
            hook.load_file(self.local_path, key, bucket, replace=True)

        else:
            raise AirflowException('Connection: {}'.format(self.conn_id))


class DynamicUploadFile(UploadFile, DynamicTargetFile):
    """Dynamic upload file operator."""


class DeleteFile(FileOperator):
    """Delete file operator."""

    def execute(self, context):
        logging.info("Deleting %s" % self.remote_path)

        if self.conn and self.conn.conn_type == "ftp":
            hook = FTPHook(self.conn_id)
            path = self._get_ftp_path(self.remote_path)
            hook.delete_file(path)

        elif self.conn and self.conn.conn_type == "s3":
            raise NotImplementedError(
                'Storage engine: {}'.format(self.conn.conn_type))

        else:
            raise AirflowException('Connection: {}'.format(self.conn_id))


class DynamicDeleteFile(DeleteFile, DynamicTargetFile):
    """Dynamic delete file operator."""
    pass
