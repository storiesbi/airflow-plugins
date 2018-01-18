from airflow.operators.slack_operator import SlackAPIPostOperator

from airflow_plugins import utils


class Message(SlackAPIPostOperator):

    """Slack message operator"""

    def __init__(self, channel=None, username=None, *args, **kwargs):

        super(Message, self).__init__(channel=channel, username=username,
                                      *args, **kwargs)

        # self.channel = self.params['company'] + "data-processing"
        self.channel = channel or "airflow_stg"

        self.token = utils.get_variable("SLACK_API_TOKEN")

        self.username = username or "Airflow (STG)"
