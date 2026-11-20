variable "project" {
  description = "GCP Project ID"
  type        = string
  default     = "project-4bb54fe6-af46-421c-bc7"
}

variable "region" {
  description = "GCP Region (us-central1 qualifies for GCS Always Free Tier 5 GB)"
  type        = string
  default     = "us-central1"
}

variable "bq_location" {
  description = "BigQuery Dataset Location (US multi-region supports direct loads from us-central1)"
  type        = string
  default     = "US"
}

variable "gcs_bucket_name" {
  description = "Globally unique Google Cloud Storage Data Lake bucket name"
  type        = string
  default     = "nordic-power-lakehouse-fi-data"
}

variable "gcs_storage_class" {
  description = "GCS Bucket Storage Class"
  type        = string
  default     = "STANDARD"
}

variable "bq_dataset_name" {
  description = "BigQuery Data Warehouse dataset name"
  type        = string
  default     = "nordic_power_analytics"
}
