import argparse
import pandas as pd

from pathlib import Path

from prefect import flow, task
from prefect_gcp import GcpCredentials
from prefect_gcp.cloud_storage import GcsBucket

@task(retries=3)
def fetch(dataset_url: str) -> pd.DataFrame:
    """Read taxi data from web into pandas DataFrame"""
    
    df = pd.read_csv(dataset_url)
    
    return df


@task(log_prints=True)
def clean(df: pd.DataFrame) -> pd.DataFrame:
    """Fix dtype issues"""

    df["tpep_pickup_datetime"] = pd.to_datetime(df["tpep_pickup_datetime"])
    df["tpep_dropoff_datetime"] = pd.to_datetime(df["tpep_dropoff_datetime"])
    
    print(df.head(2))
    print(f"columns: {df.dtypes}")
    print(f"rows: {len(df)}")
    
    return df


@task()
def write_local(df: pd.DataFrame, color: str, dataset_file: str) -> Path:
    """Write DataFrame out locally as parquet file"""

    path = Path(f"data/{color}/{dataset_file}.parquet")
    df.to_parquet(path, compression="gzip")
    
    return path


@task()
def write_gcs(path: Path, bucket: str) -> None:
    """Upload local parquet file to GCS"""
    
    gcp_credentials = GcpCredentials.load("gcp-creds")
    gcs_bucket = GcsBucket(
        bucket=bucket,
        gcp_credentials=gcp_credentials
    )
    gcs_bucket.upload_from_path(from_path=path, to_path=path)


@flow()
def etl_web_to_gcs(bucket, year, month, color) -> None:
    """The main ETL function"""

    dataset_file = f"{color}_tripdata_{year}-{month:02}"
    dataset_url = f"https://github.com/DataTalksClub/nyc-tlc-data/releases/download/{color}/{dataset_file}.csv.gz"

    df = fetch(dataset_url)
    df_clean = clean(df)
    path = write_local(df_clean, color, dataset_file)

    write_gcs(path, bucket)


@flow()
def etl_parent_flow(
    bucket: str, months: list[int] = [1, 2], year: int = 2021, color: str = "yellow", 
):
    for month in months:
        etl_web_to_gcs(bucket, year, month, color)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Transform CSV data to Parquet and Upload To GCS')

    parser.add_argument('--bucket', help='The name of the GCS bucket to upload to')

    args = parser.parse_args()

    etl_parent_flow(args.bucket)
