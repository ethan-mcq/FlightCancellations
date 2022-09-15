import requests, json
import datetime as dt
import airflow
import boto3
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator

args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email": ["YOUREMAILHERE"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": dt.timedelta(seconds=30)
}

start_date = airflow.utils.dates.days_ago(2)

def flight_scraper(url_data, bucket_name):
    print("Fetching API results")
    response = requests.get(url_data)
    json_data = response.json()
    file_name = dt.datetime.now().strftime("%Y%m%d%H%M%S")
    with open(file_name, 'w', encoding='utf-8') as json_file:
        json.dump(json_data, json_file, ensure_ascii=False)

    s3_bucket = boto3.client('s3')
    s3_bucket.upload_file(file_name, bucket_name, f"FlightInfo{file_name}")

with DAG(
    'raw_ukraineflight',
    default_args=args,
    description="test",
    schedule_interval=dt.timedelta(minutes=5),
    start_date=start_date,
    catchup=False,
    tags=['IFYOUWANTTAG'],
) as dag:
    extract_api = PythonOperator(
        task_id="extract_api",
        python_callable=flight_scraper,
        op_kwargs={
            'url_data':'WHEREYOURESCRAPING',
            'bucket_name':"YOURBUCKETNAMEHERE"},
        dag=dag
    )
    ready_task = DummyOperator(task_id='ready')

    extract_api >> ready_task
