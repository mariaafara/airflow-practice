from datetime import datetime, timedelta
import time
from airflow.models.dag import DAG
from airflow.operators.python import PythonOperator

default_args = {
    "depends_on_past": False,
    "email": ["airflow@example.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "start_date": datetime(2022, 10, 30, 13, 0, 0),
}


def fetch_data(data_interval_start, data_interval_end):
    import pandas as pd
    print("Fetching Data in process ...")
    time.sleep(5)
    print("Slept for 5 secs")
    df = pd.DataFrame([
        {"x": 1, "y": 2},
        {"x": 2, "y": 52},
        {"x": 6, "y": 7},
        {"x": 3, "y": 2},
    ])
    filename = f"data_{data_interval_start}_{data_interval_end}.csv"
    df.to_csv(filename)
    return filename


def clean_data(**context):
    print("Cleaning Data in process ...")
    time.sleep(6)
    print("Slept for 6 secs")
    filename = context["ti"].xcom_pull(task_ids="fetch_data")
    df = pd.read_csv(filename)
    print(df)


def encode_x_features():
    print("Encoding X features in process ...")
    time.sleep(2)
    print("Slept for 2 secs")


def encode_y_labels():
    print("Encoding Y labels in process ...")
    time.sleep(2)
    print("Slept for 2 secs")


def split_data():
    print("Splitting Data in process ...")
    time.sleep(4)
    print("Slept for 4 secs")


def train_model():
    print("Training model in process ...")
    time.sleep(10)
    print("Slept for 10 secs")


def evaluate_model():
    print("Evaluating model in process ...")
    time.sleep(5)
    print("Slept for  secs")


with DAG(
        dag_id="data_pipeline",
        default_args=default_args,
        catchup=False,  # True,
        schedule_interval=None,  #timedelta(minutes=2),  # every 2 minutes
        # schedule_interval="*/2 * * * *",    # every 2 minutes
#     or use timedelta dt.timedelta(days=3)

) as dag:
    t_fetch_data = PythonOperator(
        task_id="fetch_data",
        python_callable=fetch_data,
        op_kwargs={
            "data_interval_start": "{{ data_interval_start | ts_nodash}}",
            "data_interval_end": "{{ data_interval_end | ts_nodash}}"
        }
    )
    t_clean_data = PythonOperator(task_id="clean_data", python_callable=clean_data)
    t_split_data = PythonOperator(task_id="split_data", python_callable=split_data)
    t_encode_x = PythonOperator(task_id="encode_x_features", python_callable=encode_x_features)
    t_encode_y = PythonOperator(task_id="encode_y_labels", python_callable=encode_y_labels)
    t_train_model = PythonOperator(task_id="train_model", python_callable=train_model)
    t_evaluate_model = PythonOperator(task_id="evaluate_model", python_callable=evaluate_model)

    t_fetch_data >> t_clean_data >> t_split_data >> [t_encode_x, t_encode_y] >> t_train_model >> t_evaluate_model
