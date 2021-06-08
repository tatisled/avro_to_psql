package com.github.tatisled.util;

import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import org.apache.avro.Schema;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableMultimap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public class AvroUtils {

    private static final Logger LOG = LoggerFactory.getLogger(AvroUtils.class);

    public static class BigQuerySchemaMapper {
        private BigQuerySchemaMapper() {
        }

        private static final ImmutableMultimap<String, org.apache.beam.sdk.schemas.Schema.FieldType> BIG_QUERY_TO_SCHEMA_TYPES =
                ImmutableMultimap.<String, org.apache.beam.sdk.schemas.Schema.FieldType>builder()
                        .put("STRING", org.apache.beam.sdk.schemas.Schema.FieldType.STRING.withNullable(true))
                        .put("GEOGRAPHY", org.apache.beam.sdk.schemas.Schema.FieldType.STRING.withNullable(true))
                        .put("BYTES", org.apache.beam.sdk.schemas.Schema.FieldType.BYTES.withNullable(true))
                        .put("INTEGER", org.apache.beam.sdk.schemas.Schema.FieldType.INT32.withNullable(true))
                        .put("FLOAT", org.apache.beam.sdk.schemas.Schema.FieldType.FLOAT.withNullable(true))
                        .put("FLOAT64", org.apache.beam.sdk.schemas.Schema.FieldType.DOUBLE.withNullable(true))
                        .put("NUMERIC", org.apache.beam.sdk.schemas.Schema.FieldType.BYTES.withNullable(true))
                        .put("BOOLEAN", org.apache.beam.sdk.schemas.Schema.FieldType.BOOLEAN.withNullable(true))
                        .put("INT64", org.apache.beam.sdk.schemas.Schema.FieldType.INT64.withNullable(true))
                        .put("TIMESTAMP", org.apache.beam.sdk.schemas.Schema.FieldType.INT64.withNullable(true))
                        .put("RECORD", org.apache.beam.sdk.schemas.Schema.FieldType.STRING.withNullable(true))
                        .put("DATE", org.apache.beam.sdk.schemas.Schema.FieldType.INT32.withNullable(true))
                        .put("DATETIME", org.apache.beam.sdk.schemas.Schema.FieldType.STRING.withNullable(true))
                        .put("TIME", org.apache.beam.sdk.schemas.Schema.FieldType.INT64.withNullable(true))
//                        .put("STRUCT", org.apache.beam.sdk.schemas.Schema.FieldType.RECORD)
//                        .put("ARRAY", org.apache.beam.sdk.schemas.Schema.FieldType.)
                        .build();

        private static final ImmutableMultimap<org.apache.beam.sdk.schemas.Schema.FieldType, String> SCHEMA_TYPES_TO_BIG_QUERY =
                BIG_QUERY_TO_SCHEMA_TYPES.inverse();

        public static String getBigQueryTypes(org.apache.beam.sdk.schemas.Schema.FieldType type) {
            return SCHEMA_TYPES_TO_BIG_QUERY.get(type).iterator().next();
        }
    }

    public static class AvroSchemaMapper {
        private AvroSchemaMapper() {
        }

        private static final ImmutableMultimap<Schema.Type, org.apache.beam.sdk.schemas.Schema.FieldType> AVRO_TO_SCHEMA_TYPES =
                ImmutableMultimap.<Schema.Type, org.apache.beam.sdk.schemas.Schema.FieldType>builder()
                        .put(Schema.Type.STRING, org.apache.beam.sdk.schemas.Schema.FieldType.STRING)
                        .put(Schema.Type.BYTES, org.apache.beam.sdk.schemas.Schema.FieldType.BYTES)
                        .put(Schema.Type.INT, org.apache.beam.sdk.schemas.Schema.FieldType.INT32)
                        .put(Schema.Type.FLOAT, org.apache.beam.sdk.schemas.Schema.FieldType.FLOAT)
                        .put(Schema.Type.DOUBLE, org.apache.beam.sdk.schemas.Schema.FieldType.DOUBLE)
                        .put(Schema.Type.BOOLEAN, org.apache.beam.sdk.schemas.Schema.FieldType.BOOLEAN)
                        .put(Schema.Type.LONG, org.apache.beam.sdk.schemas.Schema.FieldType.INT64)
                        .put(Schema.Type.RECORD, org.apache.beam.sdk.schemas.Schema.FieldType.STRING)
                        .build();

        private static final ImmutableMultimap<org.apache.beam.sdk.schemas.Schema.FieldType, Schema.Type> SCHEMA_TYPES_TO_AVRO =
                AVRO_TO_SCHEMA_TYPES.inverse();

        public static Schema.Type getAvroTypeFromSchema(org.apache.beam.sdk.schemas.Schema.FieldType type) {
            return SCHEMA_TYPES_TO_AVRO.get(type).iterator().next();
        }

        public static org.apache.beam.sdk.schemas.Schema.FieldType getSchemaTypeFromAvro(Schema.Type type) {
            return AVRO_TO_SCHEMA_TYPES.get(type).iterator().next();
        }
    }

    public static final org.apache.beam.sdk.schemas.Schema getSchemaFromAvroSchema(org.apache.avro.Schema avroSchema) {
        org.apache.beam.sdk.schemas.Schema.Builder schemaBuilder = org.apache.beam.sdk.schemas.Schema.builder();
        avroSchema.getFields().forEach(field -> {
            schemaBuilder.addNullableField(field.name(), AvroUtils.AvroSchemaMapper.getSchemaTypeFromAvro(field.schema().getTypes().get(0).getType()));
        });
        return schemaBuilder.build();
    }

    public static Schema getAvroSchema() {
        ClassLoader classLoader = AvroUtils.class.getClassLoader();
        try {
            return new Schema.Parser().parse(new File(Objects.requireNonNull(classLoader.getResource("schema.avsc")).getFile()));
        } catch (IOException e) {
            LOG.error(e.getMessage(), e);
            return null;
        }
    }

    public static TableSchema getTableSchema(org.apache.beam.sdk.schemas.Schema schema) {
        List<TableFieldSchema> tableFieldSchemas = new ArrayList<>();
        schema.getFields().forEach(field ->
                tableFieldSchemas.add(
                        new TableFieldSchema()
                                .setName(field.getName())
                                .setType(BigQuerySchemaMapper.getBigQueryTypes(field.getType()))
                                .setMode("NULLABLE"))
        );
        return new TableSchema().setFields(tableFieldSchemas);
    }

    public static TableRow convertRecordToTableRow(
            Row row, TableSchema tableSchema) {
        List<TableFieldSchema> fields = tableSchema.getFields();
        TableRow tableRow = new TableRow();
        for (TableFieldSchema subSchema : fields) {
            org.apache.beam.sdk.schemas.Schema.Field field = row.getSchema().getField(subSchema.getName());
            if (field == null || field.getName() == null) {
                continue;
            }
            tableRow.set(field.getName(), row.getValue(field.getName()));
        }
        return tableRow;
    }

}
