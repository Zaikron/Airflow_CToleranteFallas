> # UDG - CUCEI 
> #### 13 de Marzo de 2023
### <p align="center"> Anthony Esteven Sandoval Marquez, 215660767</p>
#### <p align="center"> Materia: Computacion Tolerante a Fallas </p>
#### <p align="center"> Profesor: Michel Emanuel López Franco </p>
#### <p align="center"> Ciclo: 2023-A </p>

> ## Airflow is a platform to programmatically author, schedule and monitor workflows.

#### El código define dos DAGs en Airflow que utilizan el operador BranchDateTimeOperator para crear una rama en función de si la fecha u hora actual se encuentra dentro de un rango especificado o fuera de él.

#### En la primera DAG (example_branch_datetime_operator), se define un BranchDateTimeOperator con un target_upper y target_lower especificados como objetos datetime. El operador se utilizará para decidir si se ejecuta el operador EmptyOperator con el task_id 'date_in_range' o el task_id 'date_outside_range', dependiendo de si la hora actual está entre el rango de tiempo especificado o no.

#### En la segunda DAG (example_branch_datetime_operator_2), se define otro BranchDateTimeOperator, pero esta vez con target_upper y target_lower especificados como objetos time. En este caso, si la hora actual está entre el rango de tiempo especificado, se ejecutará el operador EmptyOperator con el task_id 'date_in_range', de lo contrario se ejecutará el operador EmptyOperator con el task_id 'date_outside_range'. 

#### Además, dado que target_lower ocurre después de target_upper, target_upper se moverá al día siguiente, por lo que la condición se evaluará para el rango de tiempo de 15:00:00 a 00:00:00 del día siguiente.
<p align="center"> <img src="https://github.com/Zaikron/Airflow_CToleranteFallas/blob/main/AirfIm/c1.PNG"/> </p>

```python

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

cond1 >> [empty_task_1, empty_task_2]

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

cond2 >> [empty_task_1, empty_task_2]

```
