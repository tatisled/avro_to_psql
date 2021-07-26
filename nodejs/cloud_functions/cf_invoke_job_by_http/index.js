const {google} = require('googleapis');

/**
 * Responds to any HTTP request.
 *
 * @param {!express:Request} req HTTP request context.
 * @param {!express:Response} resp HTTP response context.
 */
exports.invokeDataflowJob = async function (req, resp) {

    if (req.body === undefined) {
        let errorMessage = 'Body is not specified in received request.';
        console.log(errorMessage);

        resp.status(400).send(errorMessage);
        return;
    }

    const auth = new google.auth.GoogleAuth({
        // Scopes can be specified either as an array or as a single, space-delimited string.
        scopes: [
            'https://www.googleapis.com/auth/cloud-platform',
            'https://www.googleapis.com/auth/userinfo.email'
        ]
    });
    const authClient = await auth.getClient();

    // obtain the current project Id
    const project = await auth.getProjectId();
    console.log(`   Got project: ` + project)


    const dataflow = google.dataflow({version: 'v1b3', auth: authClient});

    dataflow.projects.templates.launch({
        gcsPath: req.body.template,
        location: 'us-central1',
        projectId: project,
        requestBody: {
            jobName: req.body.jobName,
            parameters: req.body.parameters
        }
    }, null, function (err, response) {
        if (err) {
            console.error("Problem occurred while running dataflow template, error was: ", err);
            resp.status(400).send(err);

        }
        console.log("Dataflow template response: ", response);
        resp.status(200).send(response);
    });

};