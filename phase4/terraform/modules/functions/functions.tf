resource "google_cloudfunctions_function" "cf_http_publish_to_pubsub" {
  name = "cf_http_publish_to_pubsub"
  description = "Cloud function to publish message to pubsub topic"
  runtime = "nodejs14"

  available_memory_mb = 256
  source_archive_bucket = google_storage_bucket.bucket_jars.name
  source_archive_object = google_storage_bucket_object.archive_cf_publish_to_pubsub_from_http.name
  trigger_http = true
  entry_point = "publishMessage"
}

resource "google_cloudfunctions_function" "cf_listen_to_pubsub_call_http" {
  name = "cf_listen_to_pubsub_call_http"
  description = "Cloud function to listen to pubsub topic and call http function to invoke a job by template"
  runtime = "nodejs14"

  available_memory_mb = 256
  source_archive_bucket = google_storage_bucket.bucket_jars.name
  source_archive_object = google_storage_bucket_object.archive_cf_invoke_job_from_pubsub.name
  event_trigger {
    event_type = "google.pubsub.topic.publish"
    resource = "projects/${var.project}/topics/${google_pubsub_topic.jobs_to_run_topic.name}"
  }
  entry_point = "onGettingMessageCallHttp"
}

resource "google_cloudfunctions_function" "cf_listen_to_http_invoke_job" {
  name = "cf_listen_to_http_invoke_job"
  description = "Cloud function to listen to http and invoke a job by template"
  runtime = "nodejs14"

  available_memory_mb = 256
  source_archive_bucket = google_storage_bucket.bucket_jars.name
  source_archive_object = google_storage_bucket_object.archive_cf_invoke_job_from_http.name
  trigger_http = true
  entry_point = "invokeDataflowJob"
}

resource "google_cloudfunctions_function" "cf_listen_to_storage_invoke_avro_to_bq" {
  depends_on = [google_storage_bucket.bucket_2]

  name = "cf_listen_to_storage_invoke_avro_to_bq"
  description = "Cloud function to listen to gcp bucket and invoke a avro_to_bq job by template"
  runtime = "nodejs14"

  available_memory_mb = 256
  source_archive_bucket = google_storage_bucket.bucket_jars.name
  source_archive_object = google_storage_bucket_object.archive_cf_invoke_avro_to_bq_from_storage.name
  event_trigger {
    event_type = "google.storage.object.finalize"
    resource = "onboardingproject_bucket_2"
  }
  entry_point = "invokeAvroToBqJob"
}