from airflow_plugins.operators import BashOperator


class GitOperator(BashOperator):
    """Base Git operator."""

    template_fields = ('bash_command', 'env', 'options')

    bash_command = """
    git {{ params.action }} {{ params.options }}
    """

    options = ""

    def __init__(self, *args, **kwargs):

        if 'params' in kwargs:
            kwargs['params'].update({'action': self.action,
                                     'options': self.options})

        super(BashOperator, self).__init__(*args, **kwargs)


class GitClone(GitOperator):
    """Git clone operator."""

    action = "clone"

    options = """
      {%- if params.source_path %}{{ params.source_path }} \{%- endif %}
      {%- if params.output_path %} {{ params.output_path }}{%- endif %}
      """


class GitCommit(GitOperator):
    """Git commit operator."""

    action = "commit"

    bash_command = """
    cd {{ params.source_path }}; git {{ params.action }} {{ params.options }}
    """

    options = """-m '{{ params.message }}'"""


class GitPush(GitOperator):
    """Git push operator."""

    action = "push"

    bash_command = """
    cd {{ params.source_path }}; git {{ params.action }} {{ params.options }}
    """
