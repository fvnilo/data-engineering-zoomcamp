terraform {
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "6.12.0"
    }
  }
}

###################################################
# We are expecting these env vars to be defined:  #
#                                                 #
# - GOOGLE_PROJECT                                #
# - GOOGLE_REGION                                 #
# - GOOGLE_CREDENTIALS                            #
###################################################
provider "google" {}

data "google_project" "current" {}

resource "google_storage_bucket" "nyc_taxi_data" {
  name          = "${var.gcs_bucket_name}-${data.google_project.current.number}"
  location      = var.location
  force_destroy = true


  lifecycle_rule {
    condition {
      age = 1
    }
    action {
      type = "AbortIncompleteMultipartUpload"
    }
  }
}

resource "google_bigquery_dataset" "nyc_taxi" {
  dataset_id = "${var.bq_dataset_name}_${data.google_project.current.number}"
  location   = var.location
}