variable "credentials" {
  description = "My Credentials"
  default     = "./keys/my-creds.json"
}

variable "region" {
  description = "Region"
  default     = "us-central1"
}

variable "project" {
  description = "Project ID"
  default     = "terraform-demo-230703-j3"
}

variable "location" {
  description = "Project Location"
  default     = "US"
}

variable "bq_dataset_name" {
  description = "My BigQuery Dataset Name"
  default     = "demo_dataset"
}

variable "gcs_bucket_name" {
  description = "My GCS Bucket Name"
  default     = "terraform-demo-230703-j3-terra-bucket"
}

variable "gcs_storage_class" {
  description = "Google Cloud Stroge Class"
  default     = "STANDARD"
}
