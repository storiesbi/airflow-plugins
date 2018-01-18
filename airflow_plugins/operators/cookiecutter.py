from airflow_plugins.operators import BashOperator


class CookiecutterOperator(BashOperator):

    """Run cookiecutter as operator."""

    bash_command = """
    cookiecutter {{ params.source_path }} -o {{ params.output_path }} \
      {%- for key, value in params.options.items() %}
      {{ key }}={{ value }} \
      {%- endfor %}
    """
