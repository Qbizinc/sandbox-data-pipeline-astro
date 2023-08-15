from abstract_task import AbstractTask
from airflow.operators.python import PythonOperator
from airflow.decorators import task

class PythonTask(AbstractTask):
    
    def create(**kwargs):
        module = __import__(kwargs['python_module'])
        
        if 'expand' in kwargs and kwargs['expand'] is not None and len(kwargs['expand']) > 0:
            return task(python_callable=module.main, op_kwargs=kwargs, multiple_outputs=False).expand(expand=kwargs['expand'])
        return PythonOperator(task_id=kwargs['task_id'], python_callable=module.main, op_kwargs=kwargs['python_kwargs'])