from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow import DAG, task
from datetime import datetime

# initializing dag
DAG_ID = "fpl_db_creation_dag"
post_con_id = "fpl_database"

def create_postgres_task (task_name, sql):
    return SQLExecuteQueryOperator(
        task_id = task_name,
        conn_id = post_con_id,
        sql = f"sql/{sql}_schema.sql"
    )

with DAG(
    dag_id=DAG_ID,
    start_date= datetime(2022,1,1),
    catchup=False,
    schedule="@daily"
) as dag:
    
    # create card schema
    card_schema = create_postgres_task("card_schema", "card")
    event_schema = create_postgres_task("event_schema", "event")
    phase_schema = create_postgres_task("phase_schema", "phase")
    players_schema = create_postgres_task("players_schema", "players")
    teams_schema = create_postgres_task("teams_schema", "teams")
    fixture_schema = create_postgres_task("fixture_schema", "fixture")
    fact_schema = create_postgres_task("fact_schema", "fact")



    # orchestrate
    card_schema >> event_schema >> phase_schema >> players_schema >> teams_schema >> fixture_schema >> fact_schema