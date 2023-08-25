"""
Dynamic DAG for DS enablement.

This DAG is generated from a YAML file, which is parsed and converted into a DAG.

It expects 4 task groups at the top level:
    - fe_task_group: Feature Engineering
    - training_task_group: Model Training
    - inference_task_group: Model Inference
    - postprocessing_task_group: Postprocessing

This is deliberate to force the developers into structure that will be consistent
across all DAGs owned by DS.

The DAG takes a boolean parameter, trainModel, which determines whether to run the
training task group or not.
"""
import json
import os

import yaml
from airflow import DAG
from airflow.models import Variable
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import BranchPythonOperator

from dynamic_dags_utils.utils import build_tasks

# Parse YAML content
with open("dags/dynamic_dags_utils/dag_driver.yml", "r", encoding="utf-8") as fh:
    dag_info = yaml.safe_load(fh)

var_keys = ["source_prefix", "stage_prefix", "target_prefix", "local_path"]
vars_dict = {key: Variable.get(key) for key in var_keys}
vars_dict_json = json.dumps(vars_dict)
print(os.getcwd())
with open("/tmp/vars.json", "w") as fh:
    fh.write(vars_dict_json)

metadata = dag_info.pop("dag_metadata")
metadata["catchup"] = False

with DAG(**metadata) as dag:
    dag_start = EmptyOperator(task_id="start", dag=dag)
    dag_end = EmptyOperator(task_id="end", dag=dag)

    #
    # @task
    # def get_vars():
    #     var_keys = ["source_prefix", "stage_prefix", "target_prefix", "local_path"]
    #     vars_dict = {key: Variable.get(key) for key in var_keys}
    #
    #     # We need these variables for use in a decorator (i.e., prior to task instantiation)
    #     # Reading them from a file at the top level of the task's file prevents us
    #     # from needed to access the metadata db every time our DAG is parsed
    #     vars_dict_json = json.dumps(vars_dict)
    #     with ("dags/data/tmp/vars.json").open("w") as fh:
    #         fh.write(vars_dict_json)
    #
    #     return vars_dict

    # get_vars_task = get_vars()

    build_tasks(dag=dag, task_hierarchy=dag_info)


    def run_training(params):
        """This function is used to determine whether to run the training task group or not."""
        if bool(params["trainModel"]):
            print("Training model...")
            return ["training_task_group.start"]
        print("Model training is skipped.")
        return ["inference_task_group.start"]


    branching = BranchPythonOperator(
        task_id="runTrainingBranch", python_callable=run_training, op_kwargs=dag.params
    )

    dag_start >> dag.task_group_dict["fe_task_group"] >> branching
    (
            branching
            >> dag.task_group_dict["training_task_group"]
            >> dag.task_group_dict["inference_task_group"]
            >> dag.task_group_dict["postprocessing_task_group"]
            >> dag_end
    )
    (
            branching
            >> dag.task_group_dict["inference_task_group"]
            >> dag.task_group_dict["postprocessing_task_group"]
            >> dag_end
    )
