# Docker and SQL

This portion of the course was an introduction to:

- Docker (and Docker Compose)
- Jupyter Notebook
- PostgreSQL

## Environment Variables

These are the required environment variables that need to be defined:

- `POSTGRES_USER`: The username to use for the database.
- `POSTGRES_PASSWORD`: The password to use for the database.
- `POSTGRES_DB`: The database name to use for the database.
- `PGADMIN_DEFAULT_EMAIL`: The default user's email for PgAdmin.
- `PGADMIN_DEFAULT_PASSWORD`: The default user's password for PgAdmin.
- `DB_USER`: The username to use to connect to the database.
- `DB_PASSWORD`: The password to use to connect to the database.
- `DB_PORT`: The port to use to connect to the database.
- `DB_NAME`: The database name to use to connect to the database.
- `TABLE_NAME`: The table to use.
- `DATASET_URL`: The `.parquet` file to download (Should be from [this website](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page).)


## Run Environment

To run the database, PgAdmin and the ingestion script in one command:

```sh
docker compose up
```

The ingestion script will run and exit, but the database and PgAdmin contains will continue to run.

## Notes

### Jupyter

- To install Jupyter: `pip install jupyter`.
- To run the jupyter server locally: `jupyter notebook`
- To convert a notebook to a python file: `jupyter nbconvert --to=script upload_data.ipynb`
  