resource "google_storage_bucket" "bucket_jars" {
  name = "onboardingproject_jars"
  force_destroy = true
}

resource "google_storage_bucket" "bucket_2" {
  name = "onboardingproject_bucket_2"
  force_destroy = true
}