package com.github.tatisled.phase3;

import com.github.tatisled.phase3.udaf.CustomCount;
import com.github.tatisled.common.util.SchemaConverter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.util.Utf8;
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.extensions.sql.SqlTransform;
import org.apache.beam.sdk.io.AvroIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.metrics.*;
import org.apache.beam.sdk.options.*;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;

import static com.github.tatisled.common.util.SchemaConverter.getAvroSchemaFromResource;
import static org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED;
import static org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.WriteDisposition.WRITE_APPEND;

public class AvroToBQ {

    private static final Logger LOG = LoggerFactory.getLogger(AvroToBQ.class);

    public interface AvroToBqOptions extends DataflowPipelineOptions {
        @Description("Input path")
        @Validation.Required
        ValueProvider<String> getInputPath();

        @SuppressWarnings("unused")
        void setInputPath(ValueProvider<String> path);

        @Description("BigQuery Table")
        @Validation.Required
        @Default.String("table")
        ValueProvider<String> getBqTable();

        @SuppressWarnings("unused")
        void setBqTable(ValueProvider<String> bqTable);

        @Description("Gcs Temp Location for Big query")
        @Validation.Required
        ValueProvider<String> getGcsTempLocation();

        @SuppressWarnings("unused")
        void setGcsTempLocation(ValueProvider<String> gcsTempLocation);
    }

    public static class RowParDo extends DoFn<GenericRecord, Row> {
        @ProcessElement
        public void processElement(ProcessContext c) {
            Schema schema = SchemaConverter.getSchema(c.element().getSchema());
            Row.Builder appRowBuilder = Row.withSchema(schema);
            schema.getFieldNames().forEach(fieldName -> {
                        if (Objects.requireNonNull(c.element()).get(fieldName) instanceof Utf8) {
                            appRowBuilder.addValues(Objects.requireNonNull(c.element()).get(fieldName).toString());
                        } else {
                            appRowBuilder.addValues(Objects.requireNonNull(c.element()).get(fieldName));
                        }
                    }
            );
            c.output(appRowBuilder.build());

        }
    }

    public static class BqRowCounterMetricsDoFn extends DoFn<Row, Row> {
        private final Counter counter = Metrics.counter("onboarding_project", "bq_row_counter");

        @ProcessElement
        public void processElement(ProcessContext context) {
            // count the elements
            counter.inc();
            context.output(context.element());
        }
    }

    private SqlTransform getSqlQuery() {
        String query = "SELECT hotel_id, customCount(id) as custom_count FROM PCOLLECTION GROUP BY hotel_id";
        return SqlTransform.query(query);
    }

    private static Schema getOutputSchema() {
        Schema.Builder builder = new Schema.Builder().addFields(Schema.Field.of("hotel_id", Schema.FieldType.INT64.withNullable(true))
                , Schema.Field.of("custom_count", Schema.FieldType.INT64).withNullable(true));
        return builder.build();
    }

    protected void run(AvroToBqOptions options) {
        Pipeline pipeline = Pipeline.create(options);

        pipeline
                .apply("Read Avro from DataStorage", AvroIO
                        .readGenericRecords(getAvroSchemaFromResource())
                        .from(options.getInputPath())
                )
                .apply("Transform data to Row", ParDo.of(new RowParDo()))
                .setRowSchema(SchemaConverter.getSchema(
                        Objects.requireNonNull(getAvroSchemaFromResource())
                ))
                .apply("Transform Rows with SQL query", getSqlQuery()
                        .registerUdaf(
                                "customCount"
                                , new CustomCount()
                        ))
                .apply("Count rows before write to BQ"
                        , ParDo.of(new BqRowCounterMetricsDoFn()))
                .setRowSchema(getOutputSchema())
                .apply("Write to BigQuery", BigQueryIO
                        .<Row>write()
                        .to(options.getBqTable())
                        .withSchema(SchemaConverter.getTableSchema(getOutputSchema()))
                        .withWriteDisposition(WRITE_APPEND)
                        .withCreateDisposition(CREATE_IF_NEEDED)
                        .withFormatFunction(row -> SchemaConverter.convertRowToTableRow(
                                row
                                , SchemaConverter.getTableSchema(getOutputSchema())
                        ))
                        .withCustomGcsTempLocation(options.getGcsTempLocation())
                );

        //https://stackoverflow.com/questions/67606930/i-am-getting-this-error-getting-severe-channel-managedchannelimpllogid-1-targe
        PipelineResult pipelineResult = pipeline.run();

        // request the metric called "bq_row_counter" in namespace called "onboarding_project"
        MetricQueryResults metrics = pipelineResult
                .metrics()
                .queryMetrics(
                        MetricsFilter.builder()
                                .addNameFilter(MetricNameFilter.named("onboarding_project", "bq_row_counter"))
                                .build());

        // print the metric value - there should be only one line because there is only one metric
        // called "bq_row_counter" in the namespace called "onboarding_project"
        for (MetricResult<Long> counter : metrics.getCounters()) {
            System.out.println(counter.getName() + ":" + counter.getAttempted());
        }
    }

    public static void main(String[] args) {
        AvroToBqOptions options =
                PipelineOptionsFactory.fromArgs(args).withValidation().as(AvroToBqOptions.class);

        new AvroToBQ().run(options);
    }

}
