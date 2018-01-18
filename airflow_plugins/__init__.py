
import logging
from datetime import timedelta

__version__ = "0.1.1"

try:
    from airflow.configuration import get
    from raven.contrib.celery import register_signal, register_logger_signal
    from raven.base import Client
except ImportError:
    pass
else:
    try:
        dsn = get("core", "sentry_dsn")
    except Exception as e:
        pass
    else:
        client = Client(dsn=dsn)

        # hook into the Celery error handler
        register_signal(client)

        register_logger_signal(client, loglevel=logging.ERROR)


DEFAULT_RETRIES = 1
DEFAULT_RETRY_DELAY = timedelta(0, 60)
