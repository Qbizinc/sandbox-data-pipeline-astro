from airflow.operators.bash import BashOperator

from .abstract_task import AbstractTask


class BashTask(AbstractTask):
    def create(**kwargs):
        return BashOperator(task_id=kwargs['task_id'], bash_command=kwargs['command'])