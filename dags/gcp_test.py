from datetime import datetime, timedelta

from airflow.models import DAG
from airflow.decorators import task, dag


@task
def say_hello():
    print("Hello!")

@dag(schedule="@daily", start_date=datetime(2021, 12, 1), catchup=False)
def hello_astronomer():
    say_hello()

hello_astronomer()