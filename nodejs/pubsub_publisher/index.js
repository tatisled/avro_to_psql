// Imports the Google Cloud client library
const {PubSub} = require('@google-cloud/pubsub');
const fs = require('fs');

// Creates a client; cache this for further use
const pubSubClient = new PubSub();
const topicName = 'projects/graphite-hook-314808/topics/data_wo_user_location_topic';

async function run() {

    // read contents of the file
    const data = fs.readFileSync('/Users/Tatiana_Slednikova/Work/js/pubsub_publisher/resource/test-dataset-event-without-user-location.json', 'UTF-8');
    // const data = fs.readFileSync('/Users/Tatiana_Slednikova/Work/js/pubsub_publisher/resource/test.json', 'UTF-8');

    // split the contents by new line
    const lines = data.split(/\r?\n/);

    // print all lines
    for (const line of lines) {
        await publishMessage(line);
        await sleep(10000);
    }
}

async function publishMessage(line) {
    const dataBuffer = Buffer.from(line);

    try {
        const messageId = await pubSubClient.topic(topicName).publish(dataBuffer);
        console.log(`Message content: ${line}.`);
        console.log(`Message ${messageId} published.`);
    } catch (error) {
        console.error(`Received error while publishing: ${error.message}`);
        process.exitCode = 1;
    }
}

async function sleep(millis) {
    return new Promise(resolve => setTimeout(resolve, millis));
}

run().catch(err => console.log(err));