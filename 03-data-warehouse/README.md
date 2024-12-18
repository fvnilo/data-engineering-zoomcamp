# Data Warehouse

## Prerequisites

- A GCS Bucket created.
- A BigQuery dataset created
- Environment variables
  - `PROJECT_ID`
  - `DATASET`
  - `GCS_BUCKET`

## Getting Started

1. `python write_sql_file.sql`. This will output a file named `big_query.sql`.

2. Execute the queries in BigQuery. Be sure to [disable the cached results](https://cloud.google.com/bigquery/docs/cached-results).

## Notes

Changes that needed to be done in the previous steps:

- Named the Terraform resources correctly.
- Cast some of the data explicitly because BigQuery was not able to detect the types automatically.
