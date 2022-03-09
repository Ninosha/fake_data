from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import psycopg2
import requests

"""
dag1 = [fetch data from api, upload to psa] per minute
"""


def psa_db():
    """
    :return: inserts data to psa db
    """
    conn = psycopg2.connect(
        host="postgres",
        database="airflow",
        user="airflow",
        password="airflow")

    cursor = conn.cursor()

    url = "https://api.citybik.es/v2/networks"

    response = requests.get(url)
    res = response.json()
    returned_value = res["networks"]
    data = "select count(*) from bikes"
    cursor.execute(data)
    num = cursor.fetchone()
    num = num[0]
    for item in returned_value:
        num += 1
        postgres_insert_query = """INSERT INTO bikes ("id", bike_id, company, url, name, city, country, longitude, latitude)
                                           VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)"""

        if item["company"] and type(item["company"]) != list:
            item["company"] = list(item["company"])

        record_to_insert = (num, item["id"], item["company"], item["href"], item["name"], item["location"]["city"],
                            item["location"]["country"], item["location"]["longitude"], item["location"]["latitude"])
        cursor.execute(postgres_insert_query, record_to_insert)
        conn.commit()


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
        'psa_dag',
        default_args=default_args,
        description='A simple tutorial DAG',
        schedule_interval="* * * * *",
        start_date=datetime(2022, 2, 25),
        catchup=False,
        tags=['V.1', '[PSA]'],
) as dag:
    PythonOperator(
        task_id='psa_to_db',
        python_callable=psa_db,
        provide_context=True,
        dag=dag
    )
