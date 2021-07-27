terraform {
  required_providers {
    google = {
      source = "hashicorp/google"
      version = "3.5.0"
    }
  }
}

provider "google" {
  credentials = file("/Users/Tatiana_Slednikova/gcp_cred/sa_onboardingproject_key.json")

  project = var.project
  region = var.region
  zone = var.zone
}

module "functions" {
  source = "./modules/functions"
}

module "templates" {
  source = "./modules/templates"
}