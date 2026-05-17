from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import clickhouse_connect
import psycopg2

default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def load_to_postgres():
    # Подключаемся к ClickHouse — читаем трансформированные данные
    ch_client = clickhouse_connect.get_client(
        host='clickhouse',
        port=8123,
        username='default',
        password='clickhouse'
    )

    # Забираем данные из marts-модели dbt
    rows = ch_client.query('SELECT * FROM default_marts.repo_stats').result_rows

    # Подключаемся к PostgreSQL — туда будем писать
    pg_conn = psycopg2.connect(
        host='postgres',
        port=5432,
        user='airflow',
        password='airflow',
        dbname='analytics'
    )
    pg_cursor = pg_conn.cursor()

    # Создаём таблицу если её нет
    pg_cursor.execute("""
        CREATE TABLE IF NOT EXISTS marts.repo_stats (
            repo_name VARCHAR,
            owner VARCHAR,
            language VARCHAR,
            description TEXT,
            stars INTEGER,
            forks INTEGER,
            open_issues INTEGER,
            watchers INTEGER,
            first_seen TIMESTAMP,
            last_updated TIMESTAMP,
            total_snapshots INTEGER
        )
    """)

    # Очищаем старые данные и вставляем свежие
    pg_cursor.execute("DELETE FROM marts.repo_stats")
    pg_cursor.executemany("""
        INSERT INTO marts.repo_stats VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
    """, rows)

    pg_conn.commit()
    pg_cursor.close()
    pg_conn.close()
    print(f'Загружено {len(rows)} строк в PostgreSQL')

with DAG(
    dag_id='load_to_postgres',
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule_interval='@hourly',
    catchup=False,
) as dag:

    load_task = PythonOperator(
        task_id='load_to_postgres',
        python_callable=load_to_postgres,
    )