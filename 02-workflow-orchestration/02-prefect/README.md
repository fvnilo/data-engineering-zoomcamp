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

## Use Deployment

More docs for Prefect Deploy [here](https://docs.prefect.io/v3/deploy/index)

1. Create a docker `worker-pool`. Here we create a Docker worker

    ```sh
    prefect work-pool create --type docker docker-work-pool
    ```

2. Start the server

    ```sh
    prefect server start
    ```

3. Start the worker

    ```sh
    prefect worker start --pool docker-work-pool
    ```

4. Run the deploy script

    ```sh
    python deploy.py
    ```

5. You can now access the deployment from the UI at `http://localhost:4200`.

## Notes

- Make sure you create a GCPCredentials block in the Prefect server. [Documentation](https://docs.prefect.io/integrations/prefect-gcp/index#authenticate-using-a-gcp-credentials-block)