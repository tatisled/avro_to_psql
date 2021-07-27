resource "null_resource" "create_job_template_psql_to_avro" {
  depends_on = [google_storage_bucket.bucket_1]

  provisioner "local-exec" {
    command = "cd ../.. \n mvn compile exec:java -Dexec.mainClass=com.github.tatisled.phase2.PsqlToAvro -Dexec.cleanupDaemonThreads=false -Dexec.args=\"--runner=DataflowRunner --project=onboardingproject-319313 --stagingLocation=gs://onboardingproject_bucket_1/staging --region=us-central1 --appName=PsqlToAvro --tempLocation=gs://onboardingproject_bucket_1/tmp --gcpTempLocation=gs://onboardingproject_bucket_1/gcp_tmp --templateLocation=gs://onboardingproject_bucket_1/custom_templates/psql_to_avro_template \" \n cd ./phase4/terraform/modules/templates/ \n"
  }
}

resource "null_resource" "create_job_template_avro_to_bq" {
  depends_on = [google_storage_bucket.bucket_1]

  provisioner "local-exec" {
    command = "cd ../.. \n mvn compile exec:java -Dexec.mainClass=com.github.tatisled.phase3.AvroToBQ -Dexec.cleanupDaemonThreads=false -Dexec.args=\"--runner=DataflowRunner --project=onboardingproject-319313 --stagingLocation=gs://onboardingproject_bucket_1/staging --region=us-central1 --appName=AvroToBQCalcite --gcsTempLocation=gs://onboardingproject_bucket_1/tmp --templateLocation=gs://onboardingproject_bucket_1/custom_templates/avro_to_bq_with_calcite_template \" \n cd ./phase4/terraform/modules/templates/ \n"
  }
}