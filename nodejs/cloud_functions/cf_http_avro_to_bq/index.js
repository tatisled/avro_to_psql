const {google} = require('googleapis');

/**
 * Responds to any HTTP request.
 *
 * @param {!express:Request} req HTTP request context.
 * @param {!express:Response} resp HTTP response context.
 */
exports.avroToBqFunc = async function (req, resp) {

    //todo 09.06.2021 set up credentials from request
    await google.auth.getApplicationDefault(function (err, authClient, projectId) {
        if (err) {
            throw err;
        }

        if (authClient.createScopedRequired && authClient.createScopedRequired()) {
            authClient = authClient.createScoped([
                'https://www.googleapis.com/auth/cloud-platform',
                'https://www.googleapis.com/auth/userinfo.email'
            ]);
            google.options({auth: authClient});
        }

        const dataflow = google.dataflow({version: 'v1b3', auth: authClient});

        dataflow.projects.templates.launch({
            gcsPath: 'gs://onboarding-bucket-1/custom_templates/new_template_from_maven.json',
            location: 'us-central1',
            projectId: 'graphite-hook-314808',
            requestBody: {
                jobName: 'auto_avro_job_from_func',
                parameters: {
                    region: 'us-central1',
                    inputPath: 'gs://onboarding-bucket-1/avro_dataset.avro',
                    bqTable: 'graphite-hook-314808:bq_dataset.table_auto_job_from_func-' + Date.now()
                }
            }
        }, null, function (err, response) {
            if (err) {
                console.error("problem running dataflow template, error was: ", err);
                resp.status(400).send(response);

            }
            console.log("Dataflow template response: ", response);
            resp.status(200).send(response);
        });
    });
};