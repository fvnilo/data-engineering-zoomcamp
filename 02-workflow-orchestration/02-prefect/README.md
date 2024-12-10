# Prefect

> Data Orchestration With Prefect

## Getting Started

1. Activate virtualenv

    ```sh
    python -m venv .venv
    source .venv/bin/activate
    ```

2. Install requirements

    ```sh
    pip install -r requirements.txt
    ```

3. Run the most interesting script:

    ```sh
    python ingest_data_gcs.py --bucket <YOUR-BUCKET-NAME>
    ```

## Notes

- Make sure you create a GCPCredentials block in the Prefect server. [Documentation](https://docs.prefect.io/integrations/prefect-gcp/index#authenticate-using-a-gcp-credentials-block)