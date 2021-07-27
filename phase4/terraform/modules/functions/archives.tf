data "archive_file" "cf_publish_to_pubsub_from_http" {
  depends_on = [google_storage_bucket.bucket_jars]

  type = "zip"
  source_dir = "../../nodejs/cloud_functions/cf_http_publish_to_pubsub/"
  output_path = "../archives/cloud_functions/cf_http_publish_to_pubsub.zip"
}

data "archive_file" "cf_invoke_job_from_pubsub" {
  depends_on = [google_storage_bucket.bucket_jars]

  type = "zip"
  source_dir = "../../nodejs/cloud_functions/cf_listen_to_pubsub_call_http/"
  output_path = "../archives/cloud_functions/cf_listen_to_pubsub_call_http.zip"
}

data "archive_file" "cf_invoke_job_from_http" {
  depends_on = [google_storage_bucket.bucket_jars]

  type = "zip"
  source_dir = "../../nodejs/cloud_functions/cf_listen_to_http_invoke_job/"
  output_path = "../archives/cloud_functions/cf_listen_to_http_invoke_job.zip"
}

data "archive_file" "cf_invoke_avro_to_bq_from_storage" {
  depends_on = [google_storage_bucket.bucket_jars]

  type = "zip"
  source_dir = "../../nodejs/cloud_functions/cf_listen_to_storage_invoke_avro_to_bq/"
  output_path = "../archives/cloud_functions/cf_listen_to_storage_invoke_avro_to_bq.zip"
}