from abstract_task import AbstractTask
from airflow.operators.bash import BashOperator

class BashTask(AbstractTask):
    def create(**kwargs):
        return BashOperator(task_id=kwargs['task_id'], bash_command=kwargs['command'])