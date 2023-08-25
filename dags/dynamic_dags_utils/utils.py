"""
Utility functions for dynamic DAGs.
"""
import sys

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.utils.task_group import TaskGroup

from dags.dynamic_dags_utils.factory import Creator


def get_vars_from_file(file_path="/tmp/vars.json"):
    """ Reads variables from a file. """
    import json

    with open(file_path, "r") as fh:
        vars_dict = json.load(fh)

    return vars_dict


def stage_in_gcs(gcp_conn_id="sandbox-data-pipeline-gcp",
                 bucket_name="qbiz-sandbox-data-pipeline",
                 source_prefix=None,
                 target_prefix=None,
                 local_path="/tmp",
                 source_files=None,
                 output_files=None
                 ):
    """ Decorator to stage files in GCS before and after task execution.
        gcp_conn_id (str): The connection ID to use when connecting to GCP.
        bucket_name (str): The name of the GCS bucket.
        source_prefix (str): The prefix of the source files in GCS.
        target_prefix (str): The prefix of the target files in GCS.
        local_path (str): The local path to which files are downloaded.
        source_files (list): The list of source files to download.
        output_files (list): The list of output files to upload.
    """

    if source_files is None:
        source_files = []
    if output_files is None:
        output_files = []

    def inner_decorator(func):
        def wrapper(*args, **kwargs):
            # Download files from GCS
            gcs = GCSHook(gcp_conn_id=gcp_conn_id)
            for source_file in source_files:
                gcs.download(bucket_name=bucket_name,
                             object_name=f"{source_prefix}/{source_file}",
                             filename=local_path + "/" + source_file)

            # Execute task
            func(*args, **kwargs)

            # Upload files to GCS
            for output_file in output_files:
                gcs.upload(bucket_name=bucket_name,
                           object_name=target_prefix + "/" + output_file,
                           filename=local_path + "/" + output_file)

        return wrapper

    return inner_decorator


def build_tasks(dag: DAG = None, parent_task_group: str = None, task_hierarchy: dict = None):
    """
    Recursively builds tasks and task groups from a YAML file.

    Args:
        dag (DAG): The Directed Acyclic Graph to which tasks are added.
        parent_task_group (str): The parent task group's name.
        task_hierarchy (dict): The hierarchical task information from YAML.
    """
    for task_group, tasks_or_name in task_hierarchy.items():
        if task_group == "tasks":
            create_and_connect_tasks(dag, parent_task_group, tasks_or_name)
        elif task_group == "name":
            pass
        else:
            create_nested_task_group(dag, parent_task_group, task_group, tasks_or_name)


def create_and_connect_tasks(dag: DAG, parent_task_group: str, tasks: list):
    """
    Creates and connects tasks within a task group.

    Args:
        dag (DAG): The Directed Acyclic Graph to which tasks are added.
        parent_task_group (str): The parent task group's name.
        tasks (list): List of tasks in the current group.
    """
    for task in tasks:
        task_obj = Creator.create_object(**task)

        if 'depends_on' in task and task['depends_on'] is not None and len(task['depends_on']) > 0:
            try:
                for dependency in task['depends_on']:
                    dag.get_task(f"{parent_task_group}.{dependency}") >> task_obj
            except Exception as e:
                print(e)
                print(f"Error in tasks, {task}, {dependency}")
                sys.exit(1)
        else:
            dag.get_task(f"{parent_task_group}.start") >> task_obj


def create_nested_task_group(dag: DAG, parent_task_group: str, task_group: str, tasks_or_name: dict):
    """
    Creates a nested task group and its tasks.

    Args:
        dag (DAG): The Directed Acyclic Graph to which tasks are added.
        parent_task_group (str): The parent task group's name.
        task_group (str): The name of the current task group.
        tasks_or_name (dict): Nested task group information or 'name' block.
    """
    with TaskGroup(group_id=task_group, dag=dag) as tg:
        start_task = EmptyOperator(task_id='start', dag=dag)

        nested_parent_group = f"{parent_task_group}.{task_group}" if parent_task_group else task_group
        build_tasks(dag, nested_parent_group, tasks_or_name)
