package com.github.tatisled;

import com.github.tatisled.util.AvroUtils;
import com.google.api.services.bigquery.model.TableSchema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.util.Utf8;
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.extensions.sql.SqlTransform;
import org.apache.beam.sdk.io.AvroIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.options.*;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;

import static com.github.tatisled.util.AvroUtils.getAvroSchema;
import static org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED;
import static org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.WriteDisposition.WRITE_APPEND;

public class AvroToBQ {

    private static final Logger LOG = LoggerFactory.getLogger(AvroToBQ.class);

    public static final org.apache.avro.Schema AVRO_SCHEMA = getAvroSchema();
    public static final Schema SCHEMA = AvroUtils.getSchemaFromAvroSchema(AVRO_SCHEMA);
    public static final TableSchema TABLE_SCHEMA = AvroUtils.getTableSchema(SCHEMA);

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
            Row.Builder appRowBuilder = Row.withSchema(SCHEMA);
            SCHEMA.getFieldNames().forEach(fieldName -> {
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

        private static SqlTransform getSqlQuery(TableSchema tableSchema) {
//        StringBuilder sbSelect = new StringBuilder();
//        StringBuilder sbGroupBy = new StringBuilder();
//
//        int count = tableSchema.getFields().size();
//        for (int i = 0; i < count; i++) {
//            TableFieldSchema field = tableSchema.getFields().get(i);
//            if (field.getName().equalsIgnoreCase("id")) {
//                sbSelect.append("MAX(id) as id");
//            } else {
//                sbSelect.append(" ").append(field.getName());
//                sbGroupBy.append(" ").append(field.getName());
//            }
//            if (i != count - 1) {
//                sbSelect.append(",");
//                if (!field.getName().equalsIgnoreCase("id")) {
//                    sbGroupBy.append(",");
//                }
//            }
//        }

//        String query = String.format("SELECT %s FROM PCOLLECTION GROUP BY (%s)", sbSelect.toString(), sbGroupBy.toString());

            //todo 08.06.2021 add any sql transformation if needed (group by is checked) but keep schema as avro schema
            String query = "SELECT * FROM PCOLLECTION";
            return SqlTransform.query(query);
        }

        protected void run(AvroToBqOptions options) {
            Pipeline pipeline = Pipeline.create(options);

            pipeline.apply("Read Avro from DataStorage", AvroIO.readGenericRecords(AVRO_SCHEMA)
                    .from(options.getInputPath()))
                    .apply("Transform data to Row", ParDo.of(new RowParDo())).setRowSchema(SCHEMA)
                    .apply("Transform Row due to SQL query", getSqlQuery(TABLE_SCHEMA))
                    .apply("Write to BigQuery", BigQueryIO.<Row>write()
                            .to(options.getBqTable())
                            .withSchema(TABLE_SCHEMA)
                            .withWriteDisposition(WRITE_APPEND)
                            .withCreateDisposition(CREATE_IF_NEEDED)
                            .withFormatFunction(row -> AvroUtils.convertRecordToTableRow(
                                    row
                                    , AvroUtils.getTableSchema(SCHEMA)
                            ))
                            .withCustomGcsTempLocation(options.getGcsTempLocation())
                    );
            //https://stackoverflow.com/questions/67606930/i-am-getting-this-error-getting-severe-channel-managedchannelimpllogid-1-targe
            pipeline.run().waitUntilFinish();
        }

        public static void main(String[] args) {
            AvroToBqOptions options =
                    PipelineOptionsFactory.fromArgs(args).withValidation().as(AvroToBqOptions.class);

            new AvroToBQ().run(options);
        }

    }
