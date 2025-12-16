output "gcs_bucket_name" {
  description = "The name of the created GCS bucket"
  value       = google_storage_bucket.data_lake.name
}

output "service_account_email" {
  description = "The email of the created Service Account"
  value       = google_service_account.airflow_dbt_sa.email
}

output "bigquery_datasets" {
  description = "Created BigQuery Datasets"
  value       = keys(google_bigquery_dataset.datasets)
}