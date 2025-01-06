# Setup

## Table of Contents

=================

- [How to setup dbt cloud with Bigquery](#setup-dbt-cloud-with-bigquery)
  - [Create a BigQuery service account](#create-a-bigquery-service-account)
  - [Create a dbt cloud project](#create-a-dbt-cloud-project)
  - [Add GitHub repository](#add-github-repository)

## Setup dbt Cloud with Bigquery

[Official documentation](https://docs.getdbt.com/guides/bigquery?step=1)

## Create a BigQuery Service Account

1. Open the [Service Account Creation Page](https://console.cloud.google.com/iam-admin/serviceaccounts/create) to create a service account for the project.

2. You can either grant the specific roles the account will need or simply use bq admin, as you'll be the sole user of both accounts and data. The good practice would be to give the least priviledges possible to the service account.

3. Go to the keys section, select "Create New Key". Select key type JSON and once you click on create it will get inmediately downloaded for you to use. 

## Create a dbt Cloud Project

1. Create a dbt cloud account from [their website](https://www.getdbt.com/) (free for solo developers)
2. Once you are logged in into dbt cloud you will be prompt to create a new project

    What is needed:
    - access to your Data Warehouse (Bigquery)
    - Admin access to your repo

3. Name the project
4. Choose Bigquery as the data warehouse
5. Upload the key you downloaded from BQ on the *create from file* option. This will fill out most fields related to the production credentials. Scroll down to the end of the page and set up your development credentials.

*Note: The dataset you'll see under the development credentials is the one you'll use to run and build your models during development. Since BigQuery's default location may not match the one you sued for your source data, it's recommended to create this schema manually to avoid multiregion errors.*
6. Click on *Test* and after that you can continue with the setup 

## Add GitHub repository

The simplest way to add the repository is to install the GH App. [Official documentation](https://docs.getdbt.com/docs/dbt-cloud/cloud-configuring-dbt-cloud/cloud-installing-the-github-application)

*If the dbt project is in a subdirectory, make sure you specify the __Project subdirectory__ property*
