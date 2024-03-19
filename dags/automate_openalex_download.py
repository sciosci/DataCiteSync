from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

# Define the default arguments for the DAG
default_args = {
    'owner': 'soslab',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG, its scheduling, and other properties
dag = DAG(
    'openalex_sync_dag',
    default_args=default_args,
    description='Sync OpenAlex S3 bucket weekly',
    schedule_interval='@weekly',  # This schedules the task to run weekly
    start_date=datetime(2024, 1, 1),  # Adjust start date to your needs
    catchup=False,
)
suffix = (datetime.now()).strftime("%Y%m%d%H%M")
path = f"/home/ubuntu/mypetalibrary/pmoa-cite-dataset/open_alex_metadata/openalex-snapshot-{suffix}"
bash_command = f'aws s3 sync "s3://openalex" "{path}" --no-sign-request'
# Define the task to sync the S3 bucket
sync_s3_bucket = BashOperator(
    task_id='sync_openalex_s3',
    bash_command=bash_command,
    dag=dag,
)
