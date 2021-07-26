package com.github.tatisled.phase1;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileStream;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.AvroIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.io.IOException;

public class AvroToBucket {
    public static final Logger LOG = LoggerFactory.getLogger(AvroToBucket.class);

    public static final String AVRO_FILE = "/Users/Tatiana_Slednikova/Downloads/test-dataset.avro";
    public static final String GC_BUCKET = "gs://onboardingproject-bucket1";
    public static final String OUTPUT_FILE_NAME = "avro_dataset";


    public static void main(String[] args) throws IOException {
        PipelineOptions options = PipelineOptionsFactory.create();
        Pipeline p = Pipeline.create(options);

        Schema schema = new DataFileStream<GenericRecord>(new FileInputStream(AVRO_FILE), new GenericDatumReader<>()).getSchema();
        LOG.info("Avro file schema : " + schema);

        p.apply(AvroIO.readGenericRecords(schema)
                .from(AVRO_FILE))
                .apply(AvroIO.writeGenericRecords(schema)
                        .to(GC_BUCKET)
                        .withShardNameTemplate(OUTPUT_FILE_NAME)
                        .withSuffix(".avro")
                        .withNumShards(1));

        p.run().waitUntilFinish();
    }
}
