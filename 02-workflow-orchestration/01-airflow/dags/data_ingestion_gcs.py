import os
import requests

from datetime import datetime

from airflow import DAG
from airflow.decorators import task
from airflow.utils.dates import days_ago
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateExternalTableOperator
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator


PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
BUCKET = os.environ.get("GCP_GCS_BUCKET")
BIGQUERY_DATASET = os.environ.get("GCP_BIGQUERY_DATASET")

DATASET_URL_FORMAT = "https://d37ci6vzurychx.cloudfront.net/trip-data/{}"
DATASET_FILE_FORMAT = "yellow_tripdata_{}.parquet"

PATH_TO_LOCAL_HOME = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")

default_args = {
    "owner": "airflow",
    "start_date": days_ago(1),
    "depends_on_past": False,
    "retries": 1,
}

with DAG(
    dag_id="data_ingestion_gcs",
    schedule_interval="@daily",
    default_args=default_args,
    catchup=False,
    max_active_runs=1,
    params={"run_date": None},
    tags=['dtc-de'],
) as dag:
    
    @task()
    def get_dataset_file(run_date=None):
        """
        Determines the dataset file to use based on the provided DAG run configuration
        or defaults to the current month.
        """
        if not run_date:
            # Default to current month dataset if no config provided
            current_month = datetime.now().strftime("%Y-%m")
            return DATASET_FILE_FORMAT.format(current_month)
        
        return DATASET_FILE_FORMAT.format(run_date)

    @task()
    def download_dataset(dataset_file: str):
        dataset_url = DATASET_URL_FORMAT.format(dataset_file)
        local_filepath = f"{PATH_TO_LOCAL_HOME}/{dataset_file}"

        response = requests.get(dataset_url, stream=True)
        response.raise_for_status()

        with open(local_filepath, "wb") as f:
            for chunk in response.iter_content(chunk_size=8192):
                f.write(chunk)

        return local_filepath

    upload_to_gcs = LocalFilesystemToGCSOperator(
        task_id="upload_to_gcs",
        src="{{ task_instance.xcom_pull(task_ids='download_dataset') }}",
        dst="raw/{{ task_instance.xcom_pull(task_ids='get_dataset_file') }}",
        bucket=BUCKET,
    )

    create_bigquery_table = BigQueryCreateExternalTableOperator(
        task_id="create_bigquery_table",
        table_resource={
            "tableReference": {
                "projectId": PROJECT_ID,
                "datasetId": BIGQUERY_DATASET,
                "tableId": "external_table",
            },
            "externalDataConfiguration": {
                "sourceFormat": "PARQUET",
                "sourceUris": [
                    f"gs://{BUCKET}/raw/{{ task_instance.xcom_pull(task_ids='get_dataset_file') }}"
                ],
            },
        },
    )

    run_date = "{{ params.run_date }}"
    dataset_file = get_dataset_file(run_date=run_date)
    downloaded_file = download_dataset(dataset_file)
    downloaded_file >> upload_to_gcs >> create_bigquery_table
