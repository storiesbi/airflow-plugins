import pytest
from airflow.models import Variable

from airflow_plugins.variables import ValueResolver


@pytest.mark.parametrize('variables_fixture', [
    (
        {
            'variables': {
                'verbosity': "",
            },
            'variable_to_delete': ["verbosity"]
        }
    )
], indirect=True)
def test_mis_configured_variable(variables_fixture):
    Variable.set("verbosity", "")

    with pytest.raises(RuntimeError) as err:
        ValueResolver.get_value("verbosity")

    assert str(err.value) == "Variable `verbosity` exists, but " \
                             "is mis-configured."


def test_not_existing_variable():
    with pytest.raises(RuntimeError) as err:
        ValueResolver.get_value("not-existing")

    assert str(err.value) == "Variable `not-existing` doesn't exist."


def test_default_value():
    value = ValueResolver.get_value("verbosity", default_value="INFO")
    assert value == "INFO"


@pytest.mark.parametrize('variables_fixture', [
    (
        {
            'variables': {
                'verbosity': "INFO",
                'stories_verbosity': "DEBUG",
                'test_master_verbosity': "WARNING",
            },
            'variable_to_delete': [
                "verbosity", "stories_verbosity", "test_master_verbosity",
                "stories_test_master_verbosity"
            ]
        }
    )
], indirect=True)
def test_resolve(variables_fixture):
    # dag
    value = ValueResolver.get_value("verbosity", dag="test_master")
    assert value == "WARNING"

    # dag value has higher priority than company value
    value = ValueResolver.get_value("verbosity", "stories", "test_master")
    assert value == "WARNING"

    # company value
    value = ValueResolver.get_value("verbosity", "stories")
    assert value == "DEBUG"

    # company dag value
    Variable.set("stories_test_master_verbosity", "ERROR")
    value = ValueResolver.get_value("verbosity", "stories", "test_master")
    assert value == "ERROR"

    # global value
    value = ValueResolver.get_value("verbosity")
    assert value == "INFO"


@pytest.mark.parametrize('variables_fixture', [
    (
        {
            'variables': {
                'verbosity': "INFO\n",
                'vm_size': "t2.medium	",
            },
            'variable_to_delete': [
                "verbosity", "vm_size",
            ]
        }
    )
], indirect=True)
def test_resolve_without_special_chars(variables_fixture):
    value = ValueResolver.get_value("verbosity")
    assert value == "INFO"

    value = ValueResolver.get_value("vm_size")
    assert value == "t2.medium"


@pytest.mark.parametrize('variables_fixture', [
    (
        {
            'variables': {
                'boolvalue': "true",
            },
            'variable_to_delete': [
                "boolvalue",
            ]
        }
    )
], indirect=True)
def test_boolean_values(variables_fixture):
    assert ValueResolver.get_value("boolvalue", default_value=False) is True


@pytest.mark.parametrize('variables_fixture', [
    (
        {
            'variables': {
                'intvalue': "-10 ",  # space intentional
                'floatvalue': "2.2",
            },
            'variable_to_delete': [
                'intvalue',
                'floatvalue',
            ]
        }
    )
], indirect=True)
def test_numerical_values(variables_fixture):
    def resolver(key):
        def resolve(default):
            return ValueResolver.get_value(key, default_value=default)
        return resolve

    resolve = resolver("intvalue")
    assert resolve(default=1) == -10
    assert resolve(default=1.0) == -10.0
    assert resolve(default=None) == "-10"  # strips spaces

    resolve = resolver("floatvalue")
    assert resolve(default=1) == "2.2"  # float strings cannot be casted to int
    assert resolve(default=1.0) == 2.2
    assert resolve(default=None) == "2.2"


@pytest.mark.parametrize(['variables_fixture', 'date_variable'], [
    (
        {
            'variables': {var_name: date_str},
            'variable_to_delete': [var_name],
        },
        {
            'variable': {'key': var_name, 'value': date_str},
            'should_resolve': should_resolve,
        }
    ) for var_name, date_str, should_resolve in [
        ('date', '2016-06-24', False),  # needs separator char before 'date'
        ('start_date', '2016-06-24', True),
        ('iso_date', '2016-06-24T12:24:48', True),
    ]
], indirect=['variables_fixture'])
def test_date_values(variables_fixture, date_variable):
    value = date_variable['variable']['value']
    resolved = ValueResolver.get_value(date_variable['variable']['key'])
    if date_variable['should_resolve']:
        if 'T' in value:
            assert value == resolved.isoformat().split('.')[0]
        else:
            assert value == resolved.isoformat().split('T')[0]
    else:
        assert value == resolved
