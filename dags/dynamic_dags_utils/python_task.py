from importlib import import_module

from airflow.decorators import task
from airflow.operators.python import PythonOperator

from dags.dynamic_dags_utils.abstract_task import AbstractTask


class PythonTask(AbstractTask):
    """Returns an 'expandable' PythonOperator object."""

    def create(**kwargs):
        module = import_module("dags.dynamic_dags_utils." + kwargs["python_module"])

        if (
            "expand" in kwargs
            and kwargs["expand"] is not None
            and len(kwargs["expand"]) > 0
        ):
            return task(
                python_callable=module.main, op_kwargs=kwargs, multiple_outputs=False
            ).expand(expand=kwargs["expand"])
        return PythonOperator(
            task_id=kwargs["task_id"],
            python_callable=module.main,
            op_kwargs=kwargs["python_kwargs"],
        )
