terraform {
  required_version = ">= 1.5.0"

  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 5.0"
    }
  }
}

provider "google" {
  project = var.project
  region  = var.region
}

# Google Cloud Storage Data Lake Bucket (Bronze / Silver / Resources)
resource "google_storage_bucket" "data_lake_bucket" {
  name                        = var.gcs_bucket_name
  location                    = var.region
  storage_class               = var.gcs_storage_class
  force_destroy               = true
  uniform_bucket_level_access = true

  # Automatically abort incomplete multipart uploads to avoid stray storage charges
  lifecycle_rule {
    condition {
      age = 1
    }
    action {
      type = "AbortIncompleteMultipartUpload"
    }
  }
}

# Google BigQuery Data Warehouse Dataset (Dimensions, Facts, and 360 Analytical Views)
resource "google_bigquery_dataset" "power_analytics_dataset" {
  dataset_id                  = var.bq_dataset_name
  friendly_name               = "Finland Power Generation Analytics"
  description                 = "Star schema tables and 360 analytical views for Finland ENTSO-E A73/A75/A44 power data"
  location                    = var.bq_location
  delete_contents_on_destroy  = true
}

# Optional GCP Compute Engine VM for remote Airflow deployment (disabled by default to keep cost at $0)
resource "google_compute_instance" "airflow_vm" {
  count        = var.create_vm ? 1 : 0
  name         = "nordic-power-analytics-vm"
  machine_type = var.vm_machine_type
  zone         = var.vm_zone

  boot_disk {
    initialize_params {
      image = "ubuntu-os-cloud/ubuntu-2204-lts"
      size  = 30
    }
  }

  network_interface {
    network = "default"
    access_config {}
  }

  metadata_startup_script = file("${path.module}/../start_up_script.sh")
}
