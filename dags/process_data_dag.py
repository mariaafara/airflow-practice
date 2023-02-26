import pendulum

from airflow.decorators import dag, task, task_group


@dag(
    start_date=pendulum.yesterday(),
    schedule=None,
    catchup=False,
    dag_id="process_data_dag."
)
def my_dag():
    @task
    def get_data(**context):
        # import pandas as pd
        from pathlib import Path

        from loader_storer import load_csv
        data_path = "../data/inputs/text_commands.csv"
        return load_csv(Path(data_path))[:10]  # pd.DataFrame(data=load_csv(Path(data_path)))

    @task_group
    def my_task_group(data):
        @task
        def preprocess(data, **context):
            from preprocess import Preprocessor

            preprocessor = Preprocessor(stop_words=True, stemming=True, lowercasing=True, special_chars_removal=False,
                                        numbers_removal=False, spell_check=False)
            print(data)

            preprocessed_text = preprocessor.preprocess(data["text"])
            return {"text": preprocessed_text, "label": data["label"]}

        @task
        def count(preprocessed_data, **context):
            print(len(preprocessed_data))
            return preprocessed_data

        return count(preprocess.expand(data=data))

    # @task
    # def store_data(data):
    #     from pathlib import Path
    #
    #     from loader_storer import store_csv
    #
    #     data_path = "../data/outputs/preprocessed_text_commands.csv"
    #     store_csv(Path(data_path), data)

    @task
    def store_data(data, key, bucket_name):
        from loader_storer import store_csv_s3

        store_csv_s3(key, bucket_name, data)

    store_data(my_task_group(get_data()), key="tasks/{{ti.task_id}}/{{ds}}/{{ti.try_number}}.csv",
               bucket_name="airflow_bucket")


my_dag()
