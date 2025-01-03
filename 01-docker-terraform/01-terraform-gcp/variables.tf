variable "location" {
  description = "Project Location"
  type        = string
  default     = "US"
}

variable "bq_dataset_name" {
  description = "My BigQuery Dataset Name"
  type        = string
}

variable "gcs_bucket_name" {
  description = "My Storage Bucket Name"
  type        = string
}

variable "gcs_storage_class" {
  description = "Bucket Storage Class"
  type        = string
  default     = "STANDARD"
}