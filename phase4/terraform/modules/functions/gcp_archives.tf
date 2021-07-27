resource "google_storage_bucket_object" "archive_cf_publish_to_pubsub_from_http" {
  depends_on = [google_storage_bucket.bucket_jars]

  name = "cf_http_publish_to_pubsub.zip"
  bucket = google_storage_bucket.bucket_jars.name
  source = data.archive_file.cf_publish_to_pubsub_from_http.output_path
}

resource "google_storage_bucket_object" "archive_cf_invoke_job_from_pubsub" {
  depends_on = [google_storage_bucket.bucket_jars]

  name = "cf_listen_to_pubsub_call_http.zip"
  bucket = google_storage_bucket.bucket_jars.name
  source = data.archive_file.cf_invoke_job_from_pubsub.output_path
}

resource "google_storage_bucket_object" "archive_cf_invoke_job_from_http" {
  depends_on = [google_storage_bucket.bucket_jars]

  name = "cf_listen_to_http_invoke_job.zip"
  bucket = google_storage_bucket.bucket_jars.name
  source = data.archive_file.cf_invoke_job_from_http.output_path
}

resource "google_storage_bucket_object" "archive_cf_invoke_avro_to_bq_from_storage" {
  depends_on = [google_storage_bucket.bucket_jars]

  name = "cf_listen_to_storage_invoke_avro_to_bq.zip"
  bucket = google_storage_bucket.bucket_jars.name
  source = data.archive_file.cf_invoke_avro_to_bq_from_storage.output_path
}