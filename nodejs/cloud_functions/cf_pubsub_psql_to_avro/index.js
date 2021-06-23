const https = require('https');

/**
 * Triggered from a message on a Cloud Pub/Sub topic.
 *
 * @param {object} pubsubMessage The Cloud Pub/Sub Message object.
 * @param {string} pubsubMessage.data The "data" property of the Cloud Pub/Sub Message.
 */
exports.onGettingMessageCallHttp = pubsubMessage => {
    // Print out the data from Pub/Sub, to prove that it worked
    console.log(`   Got new message from topic, message: `)
    //"template":"gs://onboarding-bucket-1/custom_templates/new_template_from_maven.json",
    // "jobName":"test_job_name_from_request",
    // "parameters":{
    //     "region":"us-central1",
    //     "inputPath":"gs://onboarding-bucket-1/avro_dataset.avro",
    //     "bqTable":"graphite-hook-314808:bq_dataset.table_auto_job_from_func-9999"

    let decodedData = Buffer.from(pubsubMessage.data, 'base64').toString();

    let jsonData = JSON.parse(decodedData);

    const data = JSON.stringify({
        template: jsonData.data.message.template,
        jobName: jsonData.data.message.jobName,
        parameters: jsonData.data.message.parameters
    });

    console.log(`   Complete data: ${data}`);
    console.log(`   Complete data.template: ${data.template}`);
    console.log(`   Complete data.jobName: ${data.jobName}`);
    console.log(`   Complete data.parameters: ${data.parameters}`);

    const options = {
        host: 'us-central1-graphite-hook-314808.cloudfunctions.net',
        // port: 8080,
        path: '/invokeDataflowJob',
        method: 'POST',
        headers: {
            'Content-Type': 'application/json',
            'Content-Length': data.length
        }
    }

    console.log(`   Complete options: ${options}`)

    const req = https.request(options, res => {
        console.log(`statusCode: ${res.statusCode}`);

        res.on('data', d => {
            console.log(`    Got response from psql_to_avro_function, response: `)
            console.log(Buffer.from(d, 'base64').toString());
        });
    });

    req.on('error', error => {
        console.log(`    Got error from psql_to_avro_function, error: `)
        console.error(error);
    });

    console.log(`   Calling psql_to_avro function by https...`);
    req.write(data);
    req.end();
};