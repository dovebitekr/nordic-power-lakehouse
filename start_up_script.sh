#!/usr/bin/env bash
set -euo pipefail

# Install Git and Docker Engine with Compose plugin on Ubuntu GCP VM
sudo apt-get update
sudo apt-get install -y git ca-certificates curl

sudo install -m 0755 -d /etc/apt/keyrings
sudo curl -fsSL https://download.docker.com/linux/ubuntu/gpg -o /etc/apt/keyrings/docker.asc
sudo chmod a+r /etc/apt/keyrings/docker.asc

echo \
  "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.asc] https://download.docker.com/linux/ubuntu \
  $(. /etc/os-release && echo "$VERSION_CODENAME") stable" | \
  sudo tee /etc/apt/sources.list.d/docker.list > /dev/null

sudo apt-get update
sudo apt-get install -y docker-ce docker-ce-cli containerd.io docker-buildx-plugin docker-compose-plugin

# Clone repository
git clone https://github.com/dovebitekr/nordic-power-lakehouse.git nordic-power-lakehouse
cd nordic-power-lakehouse

# Create GCP service account key when AIRFLOW_SERVICE_ACCOUNT_ID is set
if [ -n "${AIRFLOW_SERVICE_ACCOUNT_ID:-}" ]; then
  PROJECT_ID=$(gcloud config get-value project)
  SERVICE_ACCOUNT_EMAIL="${AIRFLOW_SERVICE_ACCOUNT_ID}@${PROJECT_ID}.iam.gserviceaccount.com"
  mkdir -p .google/credentials
  gcloud iam service-accounts keys create .google/credentials/adc.json --iam-account="$SERVICE_ACCOUNT_EMAIL"
  chmod 644 .google/credentials/adc.json
fi

# Build and launch Airflow Webserver and Scheduler
sudo docker compose up -d --build
