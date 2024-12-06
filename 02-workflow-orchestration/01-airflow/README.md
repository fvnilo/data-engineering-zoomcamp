# Airflow

> Data Orchestration With Airflow

## Setup

1. Fetch docker compose file: `curl -LfO 'https://airflow.apache.org/docs/apache-airflow/2.10.3/docker-compose.yaml'` (from https://airflow.apache.org/docs/apache-airflow/stable/howto/docker-compose/index.html)

2. Run: (this is from the docs)

    ```bash
    mkdir -p ./dags ./logs ./plugins ./config
    echo -e "AIRFLOW_UID=$(id -u)" > .env
    ```

    __Note__: In this repo, the dags folder exists.

3. Modified the build section of `&airflow-common` for:

    ```yaml
    build: 
      context: .
      args:
        BASE_AIRFLOW_IMAGE: apache/airflow:2.10.3
    ```

4. To build our image with the Google Cloud SDK, the official documentation: https://airflow.apache.org/docs/docker-stack/recipes.html#google-cloud-sdk-installation. The image can then be built with the command:

    ```bash
    docker build . \
      --pull \
      --build-arg BASE_AIRFLOW_IMAGE="apache/airflow:2.10.3" \
      --tag my-airflow-image:0.0.1
    ```

5. Initialize airflow:

    ```bash
    docker compose up airflow-init
    ```

6. If the output ends with `airflow-init-1 exited with code 0`, run `docker compose up`. This will take a few seconds. The WebUI will be available at: `http://localhost:8080`

## Notes

- Changing the environment variable `AIRFLOW__CORE__LOAD_EXAMPLES` to false will hide default examples.
- I decided to rewrite the code using Taskflow API to try out a more modern approach to writing a DAG.
- The `.env` file must also have these variables defined:
    - `GCP_PROJECT_ID`
    - `GCP_GCS_BUCKET`
    - `GCP_CREDENTIALS_FILE`
    - `GCP_BIGQUERY_DATASET`
