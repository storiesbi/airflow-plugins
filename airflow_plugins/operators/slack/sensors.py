from datetime import datetime

from airflow.operators.sensors import BaseSensorOperator
from airflow.utils.decorators import apply_defaults

from airflow_plugins.operators.slack.hooks import SlackHook


class SlackMessageSensor(BaseSensorOperator):

    """
    Executes a HTTP get statement and returns False on failure:
        404 not found or response_check function returned False

    :param http_conn_id: The connection to run the sensor against
    :type http_conn_id: string
    :param endpoint: The relative part of the full url
    :type endpoint: string
    :param params: The parameters to be added to the GET url
    :type params: a dictionary of string key/value pairs
    :param headers: The HTTP headers to be added to the GET request
    :type headers: a dictionary of string key/value pairs
    :param response_check: A check against the 'requests' response object.
        Returns True for 'pass' and False otherwise.
    :type response_check: A lambda or defined function.
    :param extra_options: Extra options for the 'requests' library, see the
        'requests' documentation (options to modify timeout, ssl, etc.)
    :type extra_options: A dictionary of options, where key is string and value
        depends on the option that's being modified.
    """

    msg_thanks = """
        Thank you {author} ! and have a nice day.
    """

    @apply_defaults
    def __init__(self,
                 channel,
                 username=None,
                 text_contains=None,
                 callback=None,
                 params=None,
                 headers=None,
                 extra_options=None, *args, **kwargs):

        super(SlackMessageSensor, self).__init__(*args, **kwargs)

        self.channel = channel
        self.username = username
        self.text_contains = text_contains
        self.params = params or {}
        self.headers = headers or {}
        self.extra_options = extra_options or {}

        self.slack = SlackHook(
            method='channels.history',
            channel=self.channel)

    def poke(self, context):

        self.slack.channel = self.slack.get_channel_id(self.slack.channel)

        try:
            response = self.slack.run()
        except Exception as e:
            raise e

        author = None

        since = self.dag.start_date

        if not since:
            since = datetime.now()

        since = since.timestamp()

        for msg in response['messages']:

            if msg['ts'] < since:
                continue

            if self.params['company'] in msg['text']:

                if 'file' in msg:

                    f = self.slack.get_file_content(
                        msg['file']['url_private_download'])

                    author = msg['username'].split("|")[-1][0:-1]

                    SlackHook(
                        channel=self.channel,
                        text="I got your file %s ..." % f[:100]).run()

                    SlackHook(
                        channel=self.channel,
                        text=self.msg_thanks.format(author=author)).run()

                    return True

        SlackHook(
            channel=self.channel,
            text="Hey @everyone, I'm still waiting !").run()

        return False
