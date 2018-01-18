import logging
from urllib.parse import urlencode

from slackclient import SlackClient

from asp import utils


def _compose_title_url(dag_run, env):
    url = utils.get_variable("airflow_url", "")
    if not url:
        url = "https://airflow-{}.stories.bi/admin/airflow/graph?".format(env)

    return url + urlencode({
        'execution_date': dag_run.execution_date,
        'dag_id': dag_run.dag_id
    })


def send_notification_from_context(context, text, title, color="danger"):
    ti = context['ti']
    send_notification(ti.get_dagrun(), text, title, color)


def send_notification(dag_run, text, title, color="danger"):
    env = utils.get_variable("airflow_environment", "stg")
    username = "Airflow ({})".format(env.upper())
    channel = "airflow_{}".format(env)

    token = utils.get_variable("SLACK_API_TOKEN")
    client = SlackClient(token)

    result = client.api_call(
        method="chat.postMessage",
        channel=channel,
        username=username,
        icon_url="https://raw.githubusercontent.com/airbnb/airflow/master/"
                 "airflow/www/static/pin_100.png",
        attachments=[
            {
                "color": color,
                "title": title,
                "title_link": _compose_title_url(dag_run, env),
                "text": text,
                "mrkdwn_in": ["text"]
            }
        ],
        link_names=True
    )

    if not result['ok']:
        logging.error("Slack API call failed ({})".format(result['error']))


def make_failure_callback(args):
    recipients = args.get("notification_recipients", [])
    recipients = ' '.join("@%s" % i for i in recipients)

    def callback(context):
        dag_run = context['dag_run']
        title = "{} failed :crying:".format(dag_run.dag_id)
        text = "DAG failed on '{}' task.".format(context['ti'].task_id)
        if recipients:
            text += "Hey, {} !".format(recipients)

        return send_notification(dag_run, text, title)

    return callback


def success_callback(context):
    dag_run = context['dag_run']
    title = "{} succeeded :notbad:".format(dag_run.dag_id)

    return send_notification(dag_run, "", title, color="good")
