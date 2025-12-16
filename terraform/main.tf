# ---------------------------------------------------------
# 0. Enable Required APIs
# ---------------------------------------------------------
resource "google_project_service" "bigquery_storage_api" {
  project = var.project_id
  service = "bigquerystorage.googleapis.com"

  # 建議設為 false，避免當你 destroy terraform 時，把整個 API 服務關掉導致其他服務掛點
  disable_on_destroy = false 
}

resource "google_project_service" "bigquery_api" {
  project = var.project_id
  service = "bigquery.googleapis.com"
  disable_on_destroy = false
}

resource "google_project_service" "iam_api" {
  project = var.project_id
  service = "iam.googleapis.com"
  disable_on_destroy = false
}

# ---------------------------------------------------------
# 1. Google Cloud Storage (Data Lake)
# ---------------------------------------------------------
resource "google_storage_bucket" "data_lake" {
  name          = "olist-datalake-${var.project_id}" # 確保全域唯一
  location      = var.location
  storage_class = var.storage_class
  
  # 開啟版本控制
  versioning {
    enabled = true
  }

  # Lifecycle Rule: 30天後轉為 Nearline (節省成本)
  lifecycle_rule {
    action {
      type = "SetStorageClass"
      storage_class = "NEARLINE"
    }
    condition {
      age = 30
    }
  }

  # 為了開發方便，允許強制刪除非空 Bucket (生產環境建議設為 false)
  force_destroy = true
}

# ---------------------------------------------------------
# 2. BigQuery Datasets (Data Warehouse Layers)
# ---------------------------------------------------------
# 使用 for_each 迴圈建立三個 Dataset，保持代碼簡潔
resource "google_bigquery_dataset" "datasets" {
  for_each   = toset(["olist_raw", "olist_staging", "olist_marts", "olist_intermediate"])
  
  dataset_id = each.key
  location   = var.location
  
  # 刪除 Dataset 時同時刪除內容 (開發環境方便，生產環境請小心)
  delete_contents_on_destroy = true 
}

# ---------------------------------------------------------
# 3. IAM & Service Account (Security for CI/CD)
# ---------------------------------------------------------
# 建立 Service Account
resource "google_service_account" "airflow_dbt_sa" {
  account_id   = "airflow-dbt-sa"
  display_name = "Service Account for Airflow and dbt"
}

# 定義需要的 Roles
locals {
  required_roles = [
    "roles/bigquery.dataEditor",
    "roles/bigquery.jobUser",
    "roles/storage.admin",
    "roles/serviceusage.serviceUsageConsumer",
    "roles/bigquery.readSessionUser",
  ]
}

# 將 Roles 賦予 Service Account
resource "google_project_iam_member" "sa_permissions" {
  for_each = toset(local.required_roles)

  project = var.project_id
  role    = each.value
  member  = "serviceAccount:${google_service_account.airflow_dbt_sa.email}"
}