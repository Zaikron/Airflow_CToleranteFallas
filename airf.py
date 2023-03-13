
"""
Example DAG demonstrating the usage of DateTimeBranchOperator with datetime as well as time objects as
targets.
"""
import pendulum

from airflow import DAG
from airflow.operators.datetime import BranchDateTimeOperator
from airflow.operators.empty import EmptyOperator

dag = DAG(
    dag_id="example_branch_datetime_operator",
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    tags=["example"],
    schedule_interval="@daily",
)

# [START howto_branch_datetime_operator]
empty_task_1 = EmptyOperator(task_id='date_in_range', dag=dag)
empty_task_2 = EmptyOperator(task_id='date_outside_range', dag=dag)

cond1 = BranchDateTimeOperator(
    task_id='datetime_branch',
    follow_task_ids_if_true=['date_in_range'],
    follow_task_ids_if_false=['date_outside_range'],
    target_upper=pendulum.datetime(2020, 10, 10, 15, 0, 0),
    target_lower=pendulum.datetime(2020, 10, 10, 14, 0, 0),
    dag=dag,
)

# Run empty_task_1 if cond1 executes between 2020-10-10 14:00:00 and 2020-10-10 15:00:00
cond1 >> [empty_task_1, empty_task_2]
# [END howto_branch_datetime_operator]


dag = DAG(
    dag_id="example_branch_datetime_operator_2",
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    tags=["example"],
    schedule_interval="@daily",
)
# [START howto_branch_datetime_operator_next_day]
empty_task_1 = EmptyOperator(task_id='date_in_range', dag=dag)
empty_task_2 = EmptyOperator(task_id='date_outside_range', dag=dag)

cond2 = BranchDateTimeOperator(
    task_id='datetime_branch',
    follow_task_ids_if_true=['date_in_range'],
    follow_task_ids_if_false=['date_outside_range'],
    target_upper=pendulum.time(0, 0, 0),
    target_lower=pendulum.time(15, 0, 0),
    dag=dag,
)

# Since target_lower happens after target_upper, target_upper will be moved to the following day
# Run empty_task_1 if cond2 executes between 15:00:00, and 00:00:00 of the following day
cond2 >> [empty_task_1, empty_task_2]
# [END howto_branch_datetime_operator_next_day]