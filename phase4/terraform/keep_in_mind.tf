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