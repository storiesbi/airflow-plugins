from copy import deepcopy
from datetime import datetime

from airflow.models import Variable
from pytz import timezone


def get_variable(key, default_var=None):
    """Returns variable from Variable or config defaults"""

    return Variable.get(key, default_var=default_var)


def create_variable(key, value):
    """Create variable"""

    return Variable.set(key, value)


def update_params(params, *args):
    d = deepcopy(params)
    for arg in args:
        d.update(deepcopy(arg))

    return d


def get_connection(conn_id):
    """Returns a connection by id
    """

    from airflow import settings, models

    session = settings.Session()

    return session.query(
        models.Connection).filter_by(
        conn_id=conn_id).first()


def delete_connection(conn_id):
    """Delete a connection by id. Return is deleted"""

    from airflow import settings, models

    session = settings.Session()

    connection = session.query(
        models.Connection).filter_by(
        conn_id=conn_id)

    deleted_rows = connection.delete()
    session.commit()

    return deleted_rows


def get_connection_str(conn_id, db_name=""):
    """Returns standard connection string
    """
    con = get_connection(conn_id)

    if con:

        return "{type}://{user}:{password}@{host}:{port}/{db_name}".format(**{
            'type': con.conn_type,
            'user': con.login,
            'password': con.password,
            'host': con.host,
            'port': con.port,
            'db_name': db_name,
        }).rstrip("/")


def get_or_create_conn(name, **kwargs):
    """Returns a connection by id
    """

    from airflow import settings, models

    session = settings.Session()

    con = get_connection(name)

    if not con:
        con = models.Connection(name, **kwargs)
        session.add(con)
        session.commit()

    return con


def get_or_update_conn(name, **kwargs):
    """Returns a connection by id
    """

    from airflow import settings, models

    session = settings.Session()

    con = get_connection(name)

    if not con:
        con = models.Connection(name, **kwargs)
        session.add(con)
        session.commit()
    else:

        for key, value in kwargs.items():

            if key == "extra":
                con.set_extra(value)
            else:
                setattr(con, key, value)

        session.commit()

    return con


def get_utc_offset(tz='Europe/Prague'):  # as hours
    tz = timezone(tz)
    utc = timezone('UTC')
    now = datetime.utcnow()
    utcnow = utc.localize(now)
    dt = utcnow.astimezone(tz).replace(tzinfo=None)
    offset = (dt - now).total_seconds() / (60 * 60)
    return offset  # float
