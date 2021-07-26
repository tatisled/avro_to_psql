terraform {
  required_providers {
    google = {
      source = "hashicorp/google"
      version = "3.5.0"
    }
  }
}

variable "project" {
  default = "onboardingproject-319313"
}

variable "region" {
  default = "us-central1"
}

variable "zone" {
  default = "us-central1-c"
}

provider "google" {
  credentials = file("/Users/Tatiana_Slednikova/gcp_cred/sa_onboardingproject_key.json")

  project = var.project
  region = var.region
  zone = var.zone
}

data "archive_file" "cf_publish_to_pubsub" {
  type = "zip"
  source_dir = "../nodejs/cloud_functions/cf_http_publish_to_pubsub/"
  output_path = "../nodejs/cloud_functions/cf_http_publish_to_pubsub/cf_http_publish_to_pubsub.zip"
}

data "archive_file" "cf_listen_to_pubsub" {
  type = "zip"
  source_dir = "../nodejs/cloud_functions/cf_http_publish_to_pubsub/"
  output_path = "../nodejs/cloud_functions/cf_http_publish_to_pubsub/cf_http_publish_to_pubsub.zip"
}

data "archive_file" "cf_invoke_job_by_http" {
  type = "zip"
  source_dir = "../nodejs/cloud_functions/cf_invoke_job_by_http/"
  output_path = "../nodejs/cloud_functions/cf_invoke_job_by_http/cf_invoke_job_by_http.zip"
}

resource "google_pubsub_topic" "jobs_to_run_topic" {
  name = "jobs_to_run_topic"
  project = var.project
}

resource "google_storage_bucket" "bucket_jars" {
  name = "onboardingproject_jars"
}

resource "google_storage_bucket" "bucket_1" {
  name = "onboardingproject_bucket_1"
  force_destroy = true
}

resource "google_storage_bucket_object" "archive_cf_http_publish_to_pubsub" {
  name = "cf_http_publish_to_pubsub.zip"
  bucket = google_storage_bucket.bucket_jars.name
  source = data.archive_file.cf_publish_to_pubsub.output_path
}

resource "google_storage_bucket_object" "archive_cf_invoke_job_by_http" {
  name = "cf_invoke_job_by_http.zip"
  bucket = google_storage_bucket.bucket_jars.name
  source = data.archive_file.cf_invoke_job_by_http.output_path
}

resource "google_cloudfunctions_function" "cf_http_publish_to_pubsub" {
  name = "cf_http_publish_to_pubsub"
  description = "Cloud function to publish message to pubsub topic"
  runtime = "nodejs14"

  available_memory_mb = 256
  source_archive_bucket = google_storage_bucket.bucket_jars.name
  source_archive_object = google_storage_bucket_object.archive_cf_http_publish_to_pubsub.name
  trigger_http = true
  entry_point = "publishMessage"
}

resource "google_cloudfunctions_function" "cf_invoke_job_by_http" {
  name = "cf_invoke_job_by_http"
  description = "Cloud function to invoke job by template"
  runtime = "nodejs14"

  available_memory_mb = 256
  source_archive_bucket = google_storage_bucket.bucket_jars.name
  source_archive_object = google_storage_bucket_object.archive_cf_invoke_job_by_http.name
  trigger_http = true
  entry_point = "invokeDataflowJob"
}

resource "null_resource" "create_job_template_psql_to_avro" {

  provisioner "local-exec" {

    command = "cd .. \n mvn compile exec:java -Dexec.mainClass=com.github.tatisled.phase2.PsqlToAvro -Dexec.cleanupDaemonThreads=false -Dexec.args=\"--runner=DataflowRunner --project=onboardingproject-319313 --stagingLocation=gs://onboardingproject_bucket_1/staging --region=us-central1 --appName=PsqlToAvro --tempLocation=gs://onboardingproject_bucket_1/tmp --gcpTempLocation=gs://onboardingproject_bucket_1/gcp_tmp --templateLocation=gs://onboardingproject_bucket_1/custom_templates/psql_to_avro_template \" \n cd phase4 \n"
  }
}

resource "null_resource" "create_job_template_avro_to_bq" {

  provisioner "local-exec" {

    command = "cd .. \n mvn compile exec:java -Dexec.mainClass=com.github.tatisled.phase3.AvroToBQ -Dexec.cleanupDaemonThreads=false -Dexec.args=\"--runner=DataflowRunner --project=onboardingproject-319313 --stagingLocation=gs://onboardingproject_bucket_1/staging --region=us-central1 --appName=AvroToBQCalcite --gcsTempLocation=gs://onboardingproject_bucket_1/tmp --templateLocation=gs://onboardingproject_bucket_1/custom_templates/avro_to_bq_with_calcite_template \" \n cd phase4 \n"
  }
}

#resource "google_cloudfunctions_function_iam_member" "invoker" {
#  project        = google_cloudfunctions_function.function.project
#  region         = google_cloudfunctions_function.function.region
#  cloud_function = google_cloudfunctions_function.function.name
#
#  role   = "roles/cloudfunctions.invoker"
#  member = "allUsers"
#}

#resource "time_sleep" "wait_30_seconds" {
#  depends_on = [null_resource.create_job_template]
#
#  destroy_duration = "30s"
#}

#resource "google_dataflow_job" "dataflow_job_psql_to_avro" {
#  depends_on = [time_sleep.wait_30_seconds]
#  name = "dataflow_job_psql_to_avro"
#  #template_gcs_path = "gs://${google_storage_bucket.bucket_1.name}/custom_templates/psql_to_avro_template.json"
#  template_gcs_path = "gs://onboardingproject_bucket_1/custom_templates/psql_to_avro_template"
#  temp_gcs_location = "gs://${google_storage_bucket.bucket_1.name}/gcp_tmp"
#  parameters = {
#    avroFileName = "avro_output"
#    bucketName = "gs://${google_storage_bucket.bucket_1.name}/psql_to_avro_output/"
#  }
#}