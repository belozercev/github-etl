from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import requests
import json
import clickhouse_connect

default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

REPOS = [
    'apache/airflow',
    'ClickHouse/ClickHouse',
    'apache/superset',
]

def extract_github_data():
    client = clickhouse_connect.get_client(host='clickhouse', port=8123)
    
    for repo in REPOS:
        url = f'https://api.github.com/repos/{repo}'
        response = requests.get(url)
        data = response.json()
        
        client.insert(
            'raw.github_repos',
            [[datetime.now(), repo, json.dumps(data)]],
            column_names=['fetched_at', 'repo_name', 'raw_data']
        )
        print(f'Загружено: {repo}, stars: {data.get("stargazers_count")}')

with DAG(
    dag_id='extract_github',
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule_interval='@hourly',
    catchup=False,
) as dag:

    extract_task = PythonOperator(
        task_id='extract_github_data',
        python_callable=extract_github_data,
    )