terraform {
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "7.16.0"
    }
  }
}

provider "google" {
  project = "terraform-demo-230703-j3"
  region  = "us-central1"
}

# Create Bucket
resource "google_storage_bucket" "demo-bucket" {
  name          = "terraform-demo-230703-j3-terra-bucket"
  location      = "US"
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