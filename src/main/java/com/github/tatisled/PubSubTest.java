package com.github.tatisled;

import com.github.tatisled.common.util.SchemaConverter;
import com.google.api.services.bigquery.model.TableSchema;
import org.apache.avro.Schema;
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.Validation;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.PCollection;
import org.joda.time.Duration;

import java.beans.PropertyVetoException;
import java.util.Objects;

import static org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED;
import static org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.WriteDisposition.WRITE_APPEND;

public class PubSubTest {
    public interface PubSubToBQOptions extends DataflowPipelineOptions {

        @Description("Topic name")
        @Validation.Required
        ValueProvider<String> getTopicName();

        void setTopicName(ValueProvider<String> topicName);

        @Description("Target Biq Query table name")
        @Validation.Required
        ValueProvider<String> getBqTableName();

        void setBqTableName(ValueProvider<String> bqTableName);

        @Description("Gcp Temp Location for Big query")
        @Validation.Required
        ValueProvider<String> getGcsTempLocation();

        @SuppressWarnings("unused")
        void setGcsTempLocation(ValueProvider<String> gcsTempLocation);

    }

    protected static PCollection<String> getPubSubMessages(Pipeline p, PubSubTest.PubSubToBQOptions options) {
        return p.apply("Read data from topic ", PubsubIO.readStrings().fromTopic(options.getTopicName()))
                .apply("Set up windowing messages for 60 sec",
                        Window.<String>into(FixedWindows.of(Duration.standardSeconds(60))))
                ;
    }

    protected static void run(PubSubTest.PubSubToBQOptions options) {
        Pipeline pipeline = Pipeline.create();

        PCollection<String> messages = getPubSubMessages(pipeline, options);

        messages
                .apply("Write encriched data to Big Query", BigQueryIO.<String>write()
                        .to(options.getBqTableName())
                        .withSchema(getOutputTableSchema())
                        .withWriteDisposition(WRITE_APPEND)
                        .withCreateDisposition(CREATE_IF_NEEDED)
                        .withFormatFunction(row -> SchemaConverter.convertRowToTableRow(
                                row
                                , getOutputTableSchema()
                        ))
                        .withCustomGcsTempLocation(options.getGcsTempLocation())
                );

        pipeline.run();
    }

    public static TableSchema getOutputTableSchema() {
        Schema avroSchema = SchemaConverter.getAvroSchemaFromResource();
        return SchemaConverter.getTableSchema(Objects.requireNonNull(avroSchema));
    }

    public static void main(String[] args) throws PropertyVetoException {
        PubSubTest.PubSubToBQOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().as(PubSubTest.PubSubToBQOptions.class);

        PubSubTest.run(options);
    }
}

