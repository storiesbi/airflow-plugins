import os.path

from airflow_plugins.operators import UnzipOperator


def test_unzip_operator():

    folder = os.path.dirname(os.path.realpath(__file__))

    op = UnzipOperator(task_id='dag_task',
                       path_to_zip_folder=folder,
                       path_to_zip_folder_pattern='*.py')

    try:
        op.execute({})
    except:
        # swallow is not zip file
        pass

    # check that the file is populated
    assert op.path_to_zip_file is not None
