from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from datetime import datetime
from pymongo import MongoClient
import pandas as pd
from sqlalchemy import create_engine

MONGO_URI = "mongodb://root:example@mongodb:27017/"
POSTGRES_URI = "postgresql://airflow:airflow@postgres:5432/airflow"

default_args = {
    'owner': 'Daniil Gaenkov',
    'start_date': datetime(2026, 3, 9),
    'retries': 1
}


def extract_and_replicate():
    client = MongoClient(MONGO_URI)
    db = client["education_db"]

    sessions = list(db.user_sessions.find())
    df_sessions = pd.DataFrame(sessions).drop(columns=['_id'])

    df_sessions['duration_sec'] = (pd.to_datetime(df_sessions['end_time']) -
                                   pd.to_datetime(df_sessions['start_time'])).dt.total_seconds()

    engine = create_engine(POSTGRES_URI)
    df_sessions.to_sql('stg_user_sessions', engine, if_exists='replace', index=False)

    tickets = list(db.support_tickets.find())
    df_tickets = pd.DataFrame(tickets).drop(columns=['_id', 'messages'])  # Очистка от вложенности
    df_tickets.to_sql('stg_support_tickets', engine, if_exists='replace', index=False)


with DAG('final_etl_pipeline', default_args=default_args, catchup=False) as dag:
    setup_db = PostgresOperator(
        task_id='setup_pg_tables',
        sql="""
            CREATE TABLE IF NOT EXISTS stg_user_sessions (user_id TEXT, duration_sec FLOAT);
            CREATE TABLE IF NOT EXISTS stg_support_tickets (ticket_id TEXT, status TEXT, issue_type TEXT);
        """
    )

    replicate_data = PythonOperator(
        task_id='replicate_mongo_to_postgres',
        python_callable=extract_and_replicate
    )

    v_user_activity = PostgresOperator(
        task_id='create_view_activity',
        sql="""
            CREATE OR REPLACE VIEW mart_user_activity AS
            SELECT user_id, COUNT(*) as session_count, AVG(duration_sec) as avg_duration
            FROM stg_user_sessions GROUP BY user_id;
        """
    )

    v_support_efficiency = PostgresOperator(
        task_id='create_view_support',
        sql="""
            CREATE OR REPLACE VIEW mart_support_efficiency AS
            SELECT status, issue_type, COUNT(*) as ticket_count
            FROM stg_support_tickets GROUP BY status, issue_type;
        """
    )

    var = setup_db >> replicate_data >> [v_user_activity, v_support_efficiency]