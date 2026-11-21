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

variable "create_vm" {
  description = "Set to true to provision an optional GCP Compute Engine VM running Airflow"
  type        = bool
  default     = false
}

variable "vm_zone" {
  description = "GCP zone for the optional Airflow VM"
  type        = string
  default     = "us-central1-a"
}

variable "vm_machine_type" {
  description = "Machine type for the optional Airflow VM"
  type        = string
  default     = "e2-standard-2"
}
