from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator

from pymongo import MongoClient
import psycopg2
import random

AD_SOYAD = "elif_ozun"

client = MongoClient("mongodb+srv://cetingokhan:cetingokhan@cluster0.ff5aw.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0")

def generate_random_heat_and_humidity_data(n):
    records = []
    for _ in range(n):
        record = {
            "temperature": random.randint(10, 40),
            "humidity": random.randint(10, 100),
            "timestamp": datetime.now(),
            "creator": AD_SOYAD
        }
        records.append(record)
    return records

def create_sample_data_on_mongodb():
    db = client["bigdata_training"]
    collection_name = f"user_coll_{AD_SOYAD}"
    collection = db[collection_name]
    records = generate_random_heat_and_humidity_data(10)
    collection.insert_many(records)
    client.close()


def copy_anomalies_into_new_collection():
    db = client["bigdata_training"]
    source = db[f"user_coll_{AD_SOYAD}"]
    target = db[f"anomalies_{AD_SOYAD}"]

    # Anomalilerin daha fazla olması, Clear the target collection before copying
    #target.delete_many({}) 
    
    anomalies = list(source.find({"temperature": {"$gt": 30}}))

    docs_to_insert = []
    for doc in anomalies:
        doc.pop("_id", None)
        doc["creator"] = AD_SOYAD
        docs_to_insert.append(doc)

    if docs_to_insert:
        target.insert_many(docs_to_insert)
    client.close()

def copy_airflow_logs_into_new_collection():
    conn = psycopg2.connect(
        host="postgres",
        port="5432",
        database="airflow",
        user="airflow",
        password="airflow"
    )
    cur = conn.cursor()

    now = datetime.now()
    one_min_ago = now - timedelta(minutes=1)

    query = """
        SELECT event, COUNT(*) as count
        FROM log
        WHERE execution_date >= %s
        GROUP BY event;
    """

    cur.execute(query, (one_min_ago,))
    rows = cur.fetchall()

    db = client["bigdata_training"]
    collection = db[f"log_{AD_SOYAD}"]

    for row in rows:
        record = {
            "event_name": row[0],
            "record_count": row[1],
            "created_at": now,
            "creator": AD_SOYAD
        }
        collection.insert_one(record)

    cur.close()
    conn.close()
    client.close()

default_args = {
    'start_date': datetime(2022, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=1)
}

with DAG(
    dag_id="homework",
    default_args=default_args,
    schedule_interval="*/5 * * * *",
    catchup=False
) as dag:

    dag_start = DummyOperator(task_id="start")
    dag_final = DummyOperator(task_id="final")

    create_sample_data = PythonOperator(
        task_id="create_sample_data",
        python_callable=create_sample_data_on_mongodb
    )

    copy_anomalies = PythonOperator(
        task_id="copy_anomalies_into_new_collection",
        python_callable=copy_anomalies_into_new_collection
    )

    insert_airflow_logs = PythonOperator(
        task_id="insert_airflow_logs_into_mongodb",
        python_callable=copy_airflow_logs_into_new_collection
    )

    dag_start >> [create_sample_data, insert_airflow_logs]
    create_sample_data >> copy_anomalies >> dag_final
    insert_airflow_logs >> dag_final
