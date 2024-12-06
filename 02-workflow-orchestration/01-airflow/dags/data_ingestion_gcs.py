import os
import requests

from datetime import datetime

from airflow.decorators import dag, task

# Environment variables
PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
BUCKET = os.environ.get("GCP_GCS_BUCKET")
BIGQUERY_DATASET = os.environ.get("GCP_BIGQUERY_DATASET")
AIRFLOW_HOME = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")

# Default DAG arguments
default_args = {
    "owner": "airflow",
    "start_date": datetime(2023, 12, 1),
    "depends_on_past": False,
    "retries": 1,
}


@dag(
    dag_id="data_ingestion_gcs",
    schedule_interval="@daily",
    default_args=default_args,
    catchup=False,
    max_active_runs=1,
    params={"run_date": None},  # Allow parameterized DAG runs
    tags=["dtc-de"],
)
def data_ingestion_dag():

    @task()
    def get_dataset_file(run_date=None):
        """
        Determines the dataset file to use based on DAG parameter `run_date`
        or defaults to the current month.
        """
        if not run_date:
            # Default to current month dataset if no param provided
            current_month = datetime.now().strftime("%Y-%m")
            return f"yellow_tripdata_{current_month}.parquet"
        
        return f"yellow_tripdata_{run_date}.parquet"

    @task()
    def download_dataset(dataset_file):
        dataset_url = f"https://d37ci6vzurychx.cloudfront.net/trip-data/{dataset_file}"
        local_filepath = f"{AIRFLOW_HOME}/{dataset_file}"
        
        response = requests.get(dataset_url, stream=True)
        response.raise_for_status()

        with open(local_filepath, "wb") as f:
            for chunk in response.iter_content(chunk_size=8192):
                f.write(chunk)

        return local_filepath

    @task()
    def upload_to_gcs_task(local_filepath, dataset_file):
        from airflow.providers.google.cloud.hooks.gcs import GCSHook

        gcs_hook = GCSHook()
        destination_path = f"raw/{dataset_file}"
        gcs_hook.upload(bucket_name=BUCKET, object_name=destination_path, filename=local_filepath)

        return f"gs://{BUCKET}/{destination_path}"

    @task()
    def create_bigquery_table_task(gcs_uri):
        from google.cloud import bigquery

        client = bigquery.Client(project=PROJECT_ID)
        table_id = f"{PROJECT_ID}.{BIGQUERY_DATASET}.external_table"

        external_config = bigquery.ExternalConfig("PARQUET")
        external_config.source_uris = [gcs_uri]
        
        table = bigquery.Table(table_id)
        table.external_data_configuration = external_config

        table = client.create_table(table, exists_ok=True)
        return table_id

    # Fetch the dataset file dynamically based on DAG parameter or default to current month
    run_date = "{{ params.run_date }}"
    dataset_file = get_dataset_file(run_date=run_date)
    local_filepath = download_dataset(dataset_file)
    gcs_uri = upload_to_gcs_task(local_filepath, dataset_file)
    create_bigquery_table_task(gcs_uri)

# Instantiate the DAG
dag = data_ingestion_dag()
