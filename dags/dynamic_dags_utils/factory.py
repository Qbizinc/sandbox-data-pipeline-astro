from bash_task import BashTask
from python_task import PythonTask
from bq_task import BigQueryTask


class Creator:
    "The Factory Class"
    @staticmethod
    def create_object(**kwargs):
        "A static method to get a concrete product"
        match kwargs["operator"]:
            case 'BashOperator':
                return BashTask.create(**kwargs)
            case 'PythonOperator':
                return PythonTask.create(**kwargs)
            case 'bigquery':
                return BigQueryTask.create(**kwargs)
        return None

if __name__ == "__main__":
    # Parse YAML content
    import yaml
    with open("../data/dag_driver.yml", "r", encoding="utf-8") as fh:
        dags_info = yaml.safe_load(fh)['dags']
        for task_info in dags_info[0]['tasks']:
            obj = Creator.create_object(**task_info)
            print(obj.type)
            obj.execute()