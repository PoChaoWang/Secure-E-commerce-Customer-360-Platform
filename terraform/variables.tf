variable "project_id" {
  description = "GCP Project ID"
  type        = string
}

variable "region" {
  description = "Region for GCP resources"
  type        = string
  default     = "asia-east1"
}

variable "location" {
  description = "Location for BigQuery and GCS"
  type        = string
  default     = "asia-east1" 
}

variable "storage_class" {
  description = "Storage class for the bucket"
  type        = string
  default     = "STANDARD"
}