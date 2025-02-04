terraform {
  required_providers {
    google = {
      source = "hashicorp/google"
      version = "4.51.0"
    }
  }
}


#provider "google" {}


provider "google" {
  project = "kranio-data-lab"
  credentials = file("credentials/kranio-data-lab-b46e531119a7.json")
  region  = "us-central1"
  zone    = "us-central1-c"
}

#terraform {
#  backend "gcs" {
#    bucket  = "kranio-data-lab-state-terraform"
#    prefix  = "terraform/state"
#  }
#}