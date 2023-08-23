"""
Utility functions for dynamic DAGs.
"""
import sys
from airflow.utils.task_group import TaskGroup
from airflow.operators.empty import EmptyOperator
from airflow import DAG
from dags.dynamic_dags_utils.factory import Creator

def build_tasks(dag:DAG=None, parent_task_group:str=None, task_hierarchy:dict=None):
    """ Builds tasks and task groups recursively from a YAML file."""
    for task_group in task_hierarchy.keys():
        if task_group == "tasks":
            for task in task_hierarchy[task_group]:
                obj = Creator.create_object(**task)
                if 'depends_on' in task and task['depends_on'] is not None \
                    and len(task['depends_on']) > 0:
                    try:
                        for dependency in task['depends_on']:
                            # The name of tasks includes the TaskGroup name, seperated with a dot.
                            dag.get_task(f"{parent_task_group}.{dependency}") >> obj
                    except Exception as e:
                        print(e)
                        print(f"Error in {task_group}, {task}, {dependency}")
                        sys.exit(1)
                else:
                    dag.get_task(f"{parent_task_group}.start") >> obj

        elif task_group == "name":
            pass
        else:
            # You get here if you've encountered a TaskGroup block.
            with TaskGroup(group_id=task_group, dag=dag) as tg:
                start = EmptyOperator(task_id='start', dag=dag)
                if parent_task_group is not None:
                    build_tasks(
                        dag,
                        f"{parent_task_group}.{task_group}",
                        task_hierarchy[task_group]
                    )
                else:
                    build_tasks(dag, task_group, task_hierarchy[task_group])
