from datetime import datetime

import yaml
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import BranchPythonOperator
from airflow.utils.task_group import TaskGroup
from airflow.utils.trigger_rule import TriggerRule

from dynamic_dags_utils.factory import Creator

# Parse YAML content
with open("include/dag_driver.yml", "r", encoding="utf-8") as fh:
    dag_info = yaml.safe_load(fh)


with DAG(
    dag_id=dag_info["dag_metadata"]["dag_id"],
    start_date=datetime.now(),
    params=dag_info["dag_metadata"]["params"],
    catchup=False,  # Set to False if you don't want historical backfill
) as dag:
    dag_start = EmptyOperator(task_id="start", dag=dag)

    with TaskGroup(group_id="fe_tasks") as fe_tasks:
        for task in dag_info["fe_task_group"]["tasks"]:
            obj = Creator.create_object(**task)

            if (
                "depends_on" in task
                and task["depends_on"] is not None
                and len(task["depends_on"]) > 0
            ):
                for dependency in task["depends_on"]:
                    # The name of tasks includes the TaskGroup name, seperated with a dot.
                    dag.get_task("fe_tasks." + dependency) >> obj
            else:
                dag_start >> obj

    with TaskGroup(group_id="training_tasks") as training_tasks:
        training_start = EmptyOperator(task_id="training_start", dag=dag)
        for task in dag_info["training_task_group"]["tasks"]:
            obj = Creator.create_object(**task)

            if (
                "depends_on" in task
                and task["depends_on"] is not None
                and len(task["depends_on"]) > 0
            ):
                for dependency in task["depends_on"]:
                    # The name of tasks includes the TaskGroup name, seperated with a dot.
                    dag.get_task("training_tasks." + dependency) >> obj
            else:
                training_start >> obj

    with TaskGroup(group_id="inference_tasks") as inference_tasks:
        inference_start = EmptyOperator(
            task_id="inference_start", trigger_rule=TriggerRule.NONE_FAILED
        )
        for task in dag_info["inference_task_group"]["tasks"]:
            obj = Creator.create_object(**task)

            if (
                "depends_on" in task
                and task["depends_on"] is not None
                and len(task["depends_on"]) > 0
            ):
                for dependency in task["depends_on"]:
                    # The name of tasks includes the TaskGroup name, seperated with a dot.
                    dag.get_task("inference_tasks." + dependency) >> obj
            else:
                inference_start >> obj

    with TaskGroup(group_id="postprocessing_tasks") as postprocessing_tasks:
        postprocessing_start = EmptyOperator(task_id="postprocessing_start")
        for task in dag_info["inference_task_group"]["tasks"]:
            obj = Creator.create_object(**task)

            if (
                "depends_on" in task
                and task["depends_on"] is not None
                and len(task["depends_on"]) > 0
            ):
                for dependency in task["depends_on"]:
                    # The name of tasks includes the TaskGroup name, seperated with a dot.
                    dag.get_task("postprocessing_tasks." + dependency) >> obj
            else:
                postprocessing_start >> obj

    # @task.branch
    def run_training(params):
        if bool(params["trainModel"]):
            print("Training model...")
            return ["training_tasks.training_start"]
        print("Model training is skipped.")
        return ["inference_tasks.inference_start"]

    branching = BranchPythonOperator(
        task_id="runTraining", python_callable=run_training, op_kwargs=dag.params
    )

    fe_tasks >> branching
    branching >> training_tasks
    training_tasks >> inference_tasks
    branching >> inference_tasks
    inference_tasks >> postprocessing_tasks
