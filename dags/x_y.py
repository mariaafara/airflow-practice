from datetime import datetime, timedelta
import time
from airflow.models.dag import DAG
from airflow.operators.python import PythonOperator

import pandas as pd

default_args = {
    "depends_on_past": False,
    "email": ["airflow@example.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "start_date": datetime(2022, 1, 1),
}


def fetch_data(data_interval_start, data_interval_end):
    print("Fetching Data in process ...")
    # generate random values
    df = pd.DataFrame([
        {"x": 1, "y": 2},
        {"x": 2, "y": 52},
        {"x": 6, "y": 7},
        {"x": 3, "y": 2},
    ])
    filename = f"files/data_{data_interval_start}_{data_interval_end}.csv"
    df.to_csv(filename)
    return filename


def sum_x(**context):
    print("Summing x ...")
    filename = context["ti"].xcom_pull(task_ids="fetch_data")
    df = pd.read_csv(filename)
    sum_x = int(df["x"].sum())
    return sum_x


def sum_y(**context):
    print("Summing y ...")
    filename = context["ti"].xcom_pull(task_ids="fetch_data")
    df = pd.read_csv(filename)
    sum_y = int(df["y"].sum())
    return sum_y


def multiply_sum_x_y(data_interval_start, data_interval_end, **context):
    print("Encoding Y labels in process ...")
    sum_x = context["ti"].xcom_pull(task_ids="sum_x")
    sum_y = context["ti"].xcom_pull(task_ids="sum_y")
    result = sum_x * sum_y
    print(f"sum_x * sum_y = {result}")
    with open(f"results/result_{data_interval_start}_{data_interval_end}.txt", "w") as f:
        f.write(f"{sum_x} * {sum_y} = {result}")


with DAG(
        dag_id="x_y_operation",
        default_args=default_args,
        schedule_interval=timedelta(minutes=2),  # every 2 minutes # schedule_interval="*/2 * * * *",    # every 2 minutes
) as dag:
    t_fetch_data = PythonOperator(
        task_id="fetch_data",
        python_callable=fetch_data,
        op_kwargs={
            "data_interval_start": "{{ data_interval_start | ts_nodash}}",
            "data_interval_end": "{{ data_interval_end | ts_nodash}}"
        }
    )
    t_sum_x = PythonOperator(task_id="sum_x", python_callable=sum_x)
    t_sum_y = PythonOperator(task_id="sum_y", python_callable=sum_y)
    t_mult_x_y = PythonOperator(
        task_id="multiply_sum_x_y",
        python_callable=multiply_sum_x_y,
        op_kwargs={
            "data_interval_start": "{{ data_interval_start | ts_nodash}}",
            "data_interval_end": "{{ data_interval_end | ts_nodash}}"
        }
    )

    t_fetch_data >> [t_sum_x, t_sum_y] >> t_mult_x_y
