/**
 *
 */
const https = require('https');

exports.autoAvroToBqFunc = (file, context) => {
    let fileName = file.name;

    if (!fileName.match('\.avro')) {
        return;
    }

    console.log(`  Event: ${context.eventId}`);
    console.log(`  Event Type: ${context.eventType}`);
    console.log(`  Bucket: ${file.bucket}`);
    console.log(`  File: ${file.name}`);
    console.log(`  Metageneration: ${file.metageneration}`);
    console.log(`  Created: ${file.timeCreated}`);
    console.log(`  Updated: ${file.updated}`);

    console.log(`  Run job to load avro to BQ from template`);

    https.get('https://us-central1-graphite-hook-314808.cloudfunctions.net/avroToBqFunc', (resp) => {
        let data = '';

        // A chunk of data has been received.
        resp.on('data', (chunk) => {
            data += chunk;
        });

        // The whole response has been received. Print out the result.
        resp.on('end', () => {
            console.log(data);
        });

    }).on("error", (err) => {
        console.log("Error: " + err.message);
    });

};

