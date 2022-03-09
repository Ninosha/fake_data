from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import pandas as pd
from sqlalchemy import create_engine

"""
dag3 = [get data from warehouse, create marts, upload marts to warehouse] per minute
"""

ENGINE = create_engine('postgresql://airflow:airflow@postgres:5432/airflow')


def marts_data():
    """
    :return: gets data from warehouse db, creates marts, inserts to marts dbs
    """
    res = pd.read_sql("SELECT * FROM bikes_warehouse order by id desc limit 665", ENGINE)
    df = res.to_dict(orient="records")
    df = pd.DataFrame(df, index=None)
    mart = pd.concat([df["country"], df["company"]], axis=1)
    mart2 = pd.concat([df["company"], df["city"], df["name"]], axis=1)
    mart3 = pd.concat([df["company"], df["url"]], axis=1)
    mart4 = pd.concat([df["city"], df["name"], df["longitude"], df["latitude"]], axis=1)
    mart5 = pd.concat([df["time"], df["bike_id"], df["longitude"], df["latitude"]], axis=1)

    marts = {
        "mart": mart,
        "mart2": mart2,
        "mart3": mart3,
        "mart4": mart4,
        "mart5": mart5
    }

    for table, df in marts.items():
        df.to_sql(table, con=ENGINE, if_exists="append", index=False)


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=10)
}

with DAG(
        'marts_dag',
        default_args=default_args,
        description='A simple tutorial DAG',
        schedule_interval="* * * * *",
        start_date=datetime(2022, 2, 25),
        catchup=False,
        tags=['[MART] V.3'],
) as dag:
    PythonOperator(
        task_id='get_warehouse',
        python_callable=marts_data,
        provide_context=True,
        dag=dag
    )
