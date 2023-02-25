import pendulum

from airflow.decorators import dag, task, task_group
from airflow.models.param import Param

@dag(
    start_date=pendulum.yesterday(),
    schedule=None,
    catchup=False,
    dag_id="dynamic_mapping_task",
    params={
        "data_to_process": []
    }
)
def my_dag():
    @task
    def get_data(**context):
        return context["params"]["data_to_process"]

    @task_group
    def my_task_group(data):
        @task
        def task1(data, **context):
            return data * 2

        @task
        def task2(data, **context):
            return data + 2

        multiple_data = task1.expand(data=data)
        return task2.expand(data=multiple_data)

    my_task_group(get_data())

my_dag()