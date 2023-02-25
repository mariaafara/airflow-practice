from airflow import DAG
from airflow.models.baseoperator import BaseOperator
from airflow.utils.context import Context
from airflow.operators.python import PythonOperator
import pendulum
from airflow.models import DagRun


def method(x, y, **context):
    print(x, y)
    print(m1(pendulum.parse(x)))
    run: DagRun = context["run"]
    x: pendulum.DateTime = context["xxxx"]
    print(run.run_id)


def m1(arg: pendulum.DateTime):
    return arg.add(hours=1)


class NewOperator(BaseOperator):
    template_fields = ["query"]

    def __init__(self, task_id: str, query: str, **kwargs):
        super().__init__(task_id=task_id, **kwargs)
        self.query = query

    def execute(self, context: Context):
        print(self.query)


dag = DAG(
    dag_id="template_dag",
    start_date=pendulum.now().add(days=-3),
    schedule_interval="@hourly",
)

# task = PythonOperator(
#     task_id="task",
#     dag=dag,
#     python_callable=method,
#     op_kwargs={
#         "x": "{{ data_interval_start }}",
#         "y": "{{ data_interval_end }}",
#     },
# )

task = NewOperator(
    task_id="new_task",
    dag=dag,
    query="SELECT * FROM DB WHERE date BETWEEN {{data_interval_start}} AND {{data_interval_end}}",
)


def _print_context(**kwargs):
    print(kwargs)


print_context = PythonOperator(
    task_id="print_context",
    python_callable=_print_context,
    dag=dag,
)


def _print_execution_date_and_variable(var1, **context):
    print(var1)
    print(context["execution_date"])


print_execution_date_and_variable = PythonOperator(
    task_id="print_execution_date_and_variable",
    python_callable=_print_execution_date_and_variable,
    dag=dag,
    op_args=["var1"],  # print_execution_date_and_variable
    # op_kwargs={"var1": "var1"}
)


def _get_date(year, month, day, hour, **_):
    print(year, month, day, hour)


get_date = PythonOperator(
    task_id="get_date",
    python_callable=_get_date,
    op_kwargs={
        "year": "{{ execution_date.year }}",
        "month": "{{ execution_date.month }}",
        "day": "{{ execution_date.day }}",
        "hour": "{{ execution_date.hour }}",
    },
    dag=dag,
)
