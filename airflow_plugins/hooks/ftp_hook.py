from airflow.contrib.hooks.ftp_hook import FTPHook as FTPHookBase


class FTPHook(FTPHookBase):

    def get_conn(self):
        super(FTPHook, self).get_conn()

        params = self.get_connection(self.ftp_conn_id)
        pasv = params.extra_dejson.get("passive", True)
        self.conn.set_pasv(pasv)

        return self.conn
