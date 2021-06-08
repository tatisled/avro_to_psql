package com.github.tatisled;

import com.github.tatisled.config.ConnectionConfig;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.AvroGenericCoder;
import org.apache.beam.sdk.io.AvroIO;
import org.apache.beam.sdk.io.jdbc.JdbcIO;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.Validation;
import org.apache.beam.sdk.options.ValueProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.beans.PropertyVetoException;
import java.util.Objects;

import static com.github.tatisled.util.AvroUtils.getAvroSchema;

public class PsqlToAvro {

    private static final Logger LOG = LoggerFactory.getLogger(PsqlToAvro.class);

    public interface PsqlToAvroOptions extends DataflowPipelineOptions {
        @Description("Avro output file name")
        @Validation.Required
        ValueProvider<String> getAvroFileName();

        @SuppressWarnings("unused")
        void setAvroFileName(ValueProvider<String> avroFileName);

        @Description("GCP output bucket name")
        @Validation.Required
        ValueProvider<String> getBucketName();

        @SuppressWarnings("unused")
        void setBucketName(ValueProvider<String> bucketName);
    }

    public static void main(String[] args) throws PropertyVetoException {
        PsqlToAvroOptions options =
                PipelineOptionsFactory.fromArgs(args).withValidation().as(PsqlToAvroOptions.class);

        JdbcIO.DataSourceConfiguration config = JdbcIO.DataSourceConfiguration.create(ConnectionConfig.getConnectionConfig());

        final Schema schema = getAvroSchema();
        final String schemaStr = Objects.requireNonNull(schema).toString();
        Pipeline pipeline = Pipeline.create(options);

        pipeline.apply("Read from Postgres table", JdbcIO.<GenericRecord>read()
                .withDataSourceConfiguration(config)
                .withQuery("select * from test_table")
                .withCoder(AvroGenericCoder.of(schema))
                //takes a serializable RowMapper<> functional interface
                //schema should be serializable or be created at lambda time
                .withRowMapper((JdbcIO.RowMapper<GenericRecord>) resultSet -> {
                    Schema parsedSchema = new Schema.Parser().parse(schemaStr);
                    GenericRecord record = new GenericData.Record(Objects.requireNonNull(parsedSchema));
                    int totalFieldsCount = parsedSchema.getFields().size();
                    int counter = 0;
                    while (counter < totalFieldsCount) {
                        record.put(counter, resultSet.getObject(counter + 1));
                        counter++;
                    }
                    return record;
                })
        )
                .apply("Write to Avro file", AvroIO.writeGenericRecords(schema)
                        .to(options.getBucketName())
                        .withShardNameTemplate(options.getAvroFileName().get())
                        .withSuffix(".avro")
                        .withNumShards(1));

        pipeline.run().waitUntilFinish();
    }
}
