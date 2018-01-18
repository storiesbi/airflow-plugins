
import json
import logging

import requests
from airflow.exceptions import AirflowException
from airflow.hooks.base_hook import BaseHook
from airflow_plugins import utils
from slackclient import SlackClient


class SlackHook(BaseHook):

    """Slack hook"""

    def __init__(self,
                 token=None,
                 method='chat.postMessage',
                 api_params=None,
                 channel=None,
                 username=None,
                 text=None,
                 attachments=None,
                 *args, **kwargs):

        self.token = token or utils.get_variable("SLACK_API_TOKEN")
        self.method = method
        self.api_params = api_params
        self.channel = channel or "airflow_stg"
        self.username = username or "Airflow (STG)"
        self.text = text
        self.attachments = attachments

        super(SlackHook, self).__init__(None, *args, **kwargs)

    @property
    def client(self):

        if not hasattr(self, "_client"):

            self._client = SlackClient(self.token)

        return self._client

    def run(self, **kwargs):
        """
        SlackAPIOperator calls will not fail even if the call is not
        unsuccessful. It should not prevent a DAG from completing in success.
        """
        if not self.api_params:
            self.construct_api_call_params(**kwargs)

        rc = self.client.api_call(self.method, **self.api_params)

        if not rc['ok']:
            logging.error("Slack API call failed ({})".format(rc['error']))
            raise AirflowException(
                "Slack API call failed: ({})".format(rc['error']))

        return rc

    def construct_api_call_params(self, **kwargs):
        self.api_params = {
            'channel': self.channel
        }

        if self.username:
            self.api_params['username'] = self.username
            self.api_params['icon_url'] = \
                'https://raw.githubusercontent.com/airbnb/airflow' \
                '/master/airflow/www/static/pin_100.png'

        if self.text:
            self.api_params['text'] = self.text

        if self.attachments:
            self.api_params['attachments'] = json.dumps(self.attachments)

        self.api_params.update(**kwargs)

    def get_file_content(self, url):
        """Returns file content
        """

        r = requests.get(url, headers={
            'Authorization': 'Bearer %s' % self.token
        })

        if r.status_code == 200:
            return r.text

    def get_channel_id(self, name):
        """Returns channel id by name
        """

        rc = self.client.api_call("channels.list")

        for d in rc['channels']:

            if d['name'] == name:
                return d['id']
