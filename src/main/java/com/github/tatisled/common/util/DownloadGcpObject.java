package com.github.tatisled.common.util;

import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;

import java.nio.charset.StandardCharsets;

public class DownloadGcpObject {

    private static final String PROJECT_ID = "onboardingproject-319313";
    private static final String BUCKET_NAME = "onboardingproject-bucket1";
    private static final String OBJECT_NAME = "schema.avsc";

    public static String downloadFileAsString() {
        // The ID of your GCP project
        // String projectId = "your-project-id";

        // The ID of your GCS bucket
        // String bucketName = "your-unique-bucket-name";

        // The ID of your GCS object
        // String objectName = "your-object-name";

        // The path to which the file should be downloaded
        // String destFilePath = "/local/path/to/file.txt";

        Storage storage = StorageOptions.newBuilder().setProjectId(PROJECT_ID).build().getService();

        Blob blob = storage.get(BlobId.of(BUCKET_NAME, OBJECT_NAME));
        byte[] bytes = blob.getContent();

        return new String(bytes, StandardCharsets.UTF_8);
//        blob.downloadTo(Paths.get(destFilePath));

//        System.out.println(
//                "Downloaded object "
//                        + objectName
//                        + " from bucket name "
//                        + bucketName
//                        + " to "
//                        + destFilePath);
    }

    /**
     * Downloading file from GCP bucket as a sequence of bytes
     *
     * @param projectId project id
     * @param bucketName bucket name
     * @param fileName file name
     * @return content of file in bytes
     */
    public static byte[] downloadFile(String projectId, String bucketName, String fileName) {
        Storage storage = StorageOptions.newBuilder().setProjectId(projectId).build().getService();
        Blob blob = storage.get(BlobId.of(bucketName, fileName));
        return blob.getContent();
    }

    /**
     * Downloading file from GCP bucket as a sequence of charsets (string)
     *
     * @param projectId project id
     * @param bucketName bucket name
     * @param fileName file name
     * @return content of file as string
     */
    public static String downloadFileAsString(String projectId, String bucketName, String fileName) {
        byte[] bytes = downloadFile(projectId, bucketName, fileName);
        return new String(bytes, StandardCharsets.UTF_8);
    }
}