import os

from ingest_data_gcs import etl_parent_flow

if __name__ == "__main__":
    bucket = os.environ.get("ETL_BUCKET")

    etl_parent_flow.deploy(
        name="ingest_data_gcs",
        work_pool_name="docker-work-pool",
        image="etl-deploy-gcs",
        push=False,
        cron="* * * * *",
        parameters={"bucket": bucket},
    )