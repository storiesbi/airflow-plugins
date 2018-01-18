import re

import six
from airflow.models import Variable
from airflow.settings import Session

VARIABLE_NAME_JOIN_CHAR = '_'


class ValueResolver(object):
    session = Session()

    chars_list = list(range(0, 32)) + list(range(127, 160))
    control_chars = ''.join(map(six.unichr, chars_list))
    control_char_re = re.compile('[%s]' % re.escape(control_chars))

    @classmethod
    def get_value(cls, key, company=None, dag=None, default_value=None):
        """
        Load value from Variables with fallback into defined default_value.
        In case default value is not set, Error could be raised if variable
        doesn't exist.
        """

        variable_names = cls._resolve_names(key, company, dag)
        rows = cls.session.query(Variable).filter(
            Variable.key.in_(variable_names)).all()

        variables = {}
        for variable in rows:
            variables[variable.key] = variable

        for variable_name in variable_names:
            if variable_name in variables:
                variable = variables[variable_name]
                break
        else:
            variable = None
            value = default_value

        if variable:
            value = variable.val
            if value is not None:
                value = value.strip()
                default_type = type(default_value)
                if default_type is bool:
                    return True if value.lower() in ['1', 'true'] else False
                for typecls in [int, float]:
                    if default_type is typecls:
                        try:
                            cls_value = typecls(value)
                        except ValueError:
                            # in case of possibly multi-type variable
                            pass
                        else:
                            return cls_value
                        break

        if (key.endswith(VARIABLE_NAME_JOIN_CHAR + 'date')
                and isinstance(value, six.string_types)):
            from datetime import datetime
            value = value.split('.')[0].replace('-', '').replace(':', '')
            value_format = '%Y%m%d' + ('T%H%M%S' if len(value) > 8 else '')
            value = datetime.strptime(value, value_format)

        if value is None:
            if variable:
                error_msg = "Variable `{}` exists, but is mis-configured."\
                    .format(variable.key)
            else:
                error_msg = "Variable `{}` doesn't exist.".format(key)

            raise RuntimeError(error_msg)

        return cls.strip_value(value)

    @staticmethod
    def _compose_variable_name(*args):
        return VARIABLE_NAME_JOIN_CHAR.join(args)

    @classmethod
    def _resolve_names(cls, key, company=None, dag=None):
        key, company, dag = [
            x.lower() if x else None for x in [key, company, dag]
        ]

        possible_variables = [key]
        if company:
            possible_variables.append(
                cls._compose_variable_name(company, key)
            )

        if dag:
            possible_variables.append(cls._compose_variable_name(dag, key))

        if company and dag:
            possible_variables.append(
                cls._compose_variable_name(company, dag, key)
            )

        possible_variables.reverse()

        return possible_variables

    @classmethod
    def strip_value(cls, value):
        if not isinstance(value, six.string_types):
            return value
        else:
            return cls.control_char_re.sub('', str(value))
