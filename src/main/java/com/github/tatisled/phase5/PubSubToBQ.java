package com.github.tatisled.phase5;

import com.github.tatisled.common.config.ConnectionConfig;
import com.github.tatisled.common.util.SchemaConverter;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.gson.Gson;
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.io.jdbc.JdbcIO;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.Validation;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.transforms.join.CoGbkResult;
import org.apache.beam.sdk.transforms.join.CoGroupByKey;
import org.apache.beam.sdk.transforms.join.KeyedPCollectionTuple;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TupleTag;
import org.joda.time.Duration;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.beans.PropertyVetoException;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static com.github.tatisled.common.util.SchemaConverter.getAvroSchemaFromGsp;
import static org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED;
import static org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.WriteDisposition.WRITE_APPEND;

public class PubSubToBQ {

    private static final Logger LOG = LoggerFactory.getLogger(PubSubToBQ.class);

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

    protected static PCollection<KV<String, String>> getPubSubMessages(Pipeline p, PubSubToBQOptions options) {
        return p.apply("Read data from topic ", PubsubIO.readStrings().fromTopic(options.getTopicName()))
                .apply("Set up windowing messages for 1 sec with lateness 5 sec",
                        Window.<String>into(FixedWindows.of(Duration.standardSeconds(1L)))
                                .withAllowedLateness(Duration.standardSeconds(5L))
                                .accumulatingFiredPanes())
                .apply("Apply Id as Key", MapElements
                        .via(new SimpleFunction<String, KV<String, String>>() {
                            public KV<String, String> apply(String message) {
                                JSONObject jsonObject = new JSONObject(message);
                                Long id = jsonObject.getLong("id");
                                return KV.of(id.toString(), message);
                            }
                        })
                );
    }

    protected static PCollection<KV<String, String>> enrichMessageWithPsqlData(PCollection<KV<String, String>> messages) throws PropertyVetoException {
        JdbcIO.DataSourceConfiguration config = JdbcIO.DataSourceConfiguration.create(ConnectionConfig.getConnectionConfig());

        return messages
//                .apply("Combine input collection by key"
//                , Combine.perKey((SerializableFunction<Iterable<String>, String>) input -> {
//                    String res = "";
//                    Iterator<String> iterator = input.iterator();
//                    while (iterator.hasNext()) {
//                        res = iterator.next();
//                    }
//                    return res;
//                }))
                .apply("Read rows from Postgres", JdbcIO.<KV<String, String>, KV<String, String>>readAll()
                .withDataSourceConfiguration(config)
                .withQuery("SELECT id, user_location_country, user_location_region, user_location_city FROM test_table where id = ?")
                .withParameterSetter((JdbcIO.PreparedStatementSetter<KV<String, String>>) (element, preparedStatement) -> {
                            JSONObject jsonObject = new JSONObject(element.getValue());
                            Long id = jsonObject.getLong("id");
                            preparedStatement.setLong(1, id);
                        }
                )
                .withRowMapper((JdbcIO.RowMapper<KV<String, String>>) resultSet -> {
                    String key = resultSet.getString("id");
                    ResultSetMetaData metadata = resultSet.getMetaData();
                    // Iterate through each column to output
                    Map<String, Object> row = IntStream
                            // Iterate through each column to output
                            .range(1, resultSet.getMetaData().getColumnCount() + 1)
                            .filter(i -> {
                                try {
                                    return !metadata.getColumnName(i).equals("id");
                                } catch (SQLException e) {
                                    return false;
                                }
                            })
                            // Output a pair with the column name and value.
                            .mapToObj(i -> {
                                // This should never fail as we're bound by the column count but this is the interface.
                                try {
                                    return KV.of(metadata.getColumnName(i), resultSet.getObject(i));
                                } catch (SQLException ignored) {
                                    ignored.printStackTrace();
                                }
                                return null;
                            })
                            // Create a map of key/value pairs
                            .collect(Collectors.toMap(KV::getKey, KV::getValue));

                    // Create a Json dictionary and return it.
                    return KV.of(key, new Gson().toJson(row));
                })
                .withCoder(KvCoder.of(StringUtf8Coder.of(), StringUtf8Coder.of()))
        );
    }

    protected static PCollection<String> joinCollections(PCollection<KV<String, String>> inputCollection, PCollection<KV<String, String>> outputCollection) {
        GroupByKey.applicableTo(inputCollection);
        GroupByKey.applicableTo(outputCollection);

        final TupleTag<String> ORIGINAL_TAG = new TupleTag<>();
        final TupleTag<String> ENRICHED_TAG = new TupleTag<>();

        return KeyedPCollectionTuple
                // Mark the two PCollections with the appropriate TupleTags
                .of(ORIGINAL_TAG, inputCollection)
                .and(ENRICHED_TAG, outputCollection)
                // Join using the internal CoGroupByKey
                .apply("Combine Streams", CoGroupByKey.create())
                .apply("Compose Final Object", ParDo.of(new DoFn<KV<String, CoGbkResult>, String>() {
                    @ProcessElement
                    public void processElement(ProcessContext c) {
                        // At this point, we have our original data and all enriched records available to process.
                        // If there are more than one record and getOnly is used, an exception will be raised.
                        // It's critical that the same key isn't used in the same window otherwise we will have duplicate
                        // original records.  When we explore natural keys for the pipeline we'll revisit this.
                        String originalMessage = c.element().getValue().getOnly(ORIGINAL_TAG);


//
                        // The output of the SQL query can return multiple records.
                        // our user_location_* fields with id
                        String additionalMessage = c.element().getValue().getOnly(ENRICHED_TAG);

                        JSONObject jsonOrig = new JSONObject(originalMessage);
                        JSONObject jsonAdd = new JSONObject(additionalMessage);

                        jsonAdd.keySet().forEach(fieldName ->
                                jsonOrig.put(fieldName, jsonAdd.get(fieldName))
                        );

                        String output = jsonOrig.toString();
                        c.output(output);
                    }
                }));

    }

    protected static void run(PubSubToBQOptions options) throws PropertyVetoException {
        Pipeline pipeline = Pipeline.create(options);

        PCollection<KV<String, String>> messages = getPubSubMessages(pipeline, options);

        PCollection<KV<String, String>> enrichedMessages = enrichMessageWithPsqlData(messages);

        PCollection<String> joinedMessages = joinCollections(messages, enrichedMessages);

        joinedMessages
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
        return SchemaConverter.getTableSchema(getAvroSchemaFromGsp());
    }

    public static void main(String[] args) throws PropertyVetoException {
        PubSubToBQOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().as(PubSubToBQOptions.class);

        PubSubToBQ.run(options);
    }
}
