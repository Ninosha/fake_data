from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from sqlalchemy import create_engine
import pandas as pd

"""
dag2 = [get data from psa, format, upload to warehouse] per minute
"""

ENGINE = create_engine('postgresql://airflow:airflow@postgres:5432/airflow')


def warehouse_data():
    """
    :return: gets data from psa, formats data, pushes to warehouse db
    """

    res = pd.read_sql("SELECT * FROM bikes order by id DESC limit 665;", ENGINE)
    df = res.to_dict(orient="records")
    df = pd.DataFrame(df)
    time = datetime.now()
    time = time.strftime("%d/%m/%Y %H:%M:%S")
    df["company"] = df["company"].replace("]", "").replace("[", "").replace("'", "")
    df["url"] = "https://api.citybik.es" + df["url"]
    df["time"] = time
    df.to_sql("bikes_warehouse", con=ENGINE, if_exists="append", index=False)


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
        'warehouse_dag',
        default_args=default_args,
        description='A simple tutorial DAG',
        schedule_interval="* * * * *",
        start_date=datetime(2022, 2, 25),
        catchup=False,
        tags=['[WareHouse] V.2'],
) as dag:
    PythonOperator(
        task_id='get_psa',
        python_callable=warehouse_data,
        provide_context=True,
        dag=dag
    )
