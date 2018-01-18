import pytest
from airflow.models import Connection, Variable
from airflow.settings import Session


def add_connection(session, connection_dict):
    for key, values in connection_dict.items():
        con = Connection(key, **values)
        session.add(con)
        session.commit()


def delete_connection(session, keys):
    query = session.query(Connection).filter(Connection.conn_id.in_(keys))
    query.delete(synchronize_session="fetch")
    session.commit()


@pytest.fixture()
def connection_fixture(request):
    connection = request.param.get('connection', {})
    connection_to_delete = request.param.get('connection_to_delete', [])

    session = Session()
    add_connection(session, connection)
    yield  # Teardown
    delete_connection(session, connection_to_delete)


def add_variables(session, variables_dict):
    for key, value in variables_dict.items():
        Variable.set(key, value)


def delete_variables(session, keys):
    query = session.query(Variable).filter(Variable.key.in_(keys))
    query.delete(synchronize_session="fetch")
    session.commit()


@pytest.fixture()
def variables_fixture(request):
    variables = request.param.get('variables', {})
    variable_to_delete = request.param.get('variable_to_delete', [])

    session = Session()
    add_variables(session, variables)
    yield  # Teardown
    delete_variables(session, variable_to_delete)
