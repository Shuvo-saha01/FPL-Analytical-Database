from airflow.providers.standard.operators.python import PythonOperator
from airflow import DAG, task
from datetime import datetime
from python_script.fpl_python_script import *
from python_script.fixture import etl_fixture
from python_script.fact import etl_fact

# initializing dag
DAG_ID = "fpl_data_update_dag"

def construct_python_operator_task(task_name, taskFunction):
    return PythonOperator(
        task_id = f"{task_name}",
        python_callable=taskFunction
    )

with DAG(
    dag_id=DAG_ID,
    start_date= datetime(2022,1,1),
    catchup=False,
    schedule="@daily"
) as dag:
    
    task_etl_cards = construct_python_operator_task("etl_cards", etl_cards)
    task_etl_events = construct_python_operator_task("etl_events", etl_events)
    task_etl_teams = construct_python_operator_task("etl_teams", etl_teams)
    task_etl_players = construct_python_operator_task("etl_players", etl_players)
    task_etl_phases = construct_python_operator_task("etl_phases", etl_phases)
    task_etl_fixture = construct_python_operator_task("etl_fixture", etl_fixture)
    task_etl_fact = construct_python_operator_task("etl_fact", etl_fact)

    task_etl_cards >> task_etl_events >> task_etl_teams >> task_etl_players >> task_etl_phases >> task_etl_fixture >> task_etl_fact