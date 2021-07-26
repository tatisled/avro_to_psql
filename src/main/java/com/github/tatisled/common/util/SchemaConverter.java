package com.github.tatisled.common.util;

import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.values.Row;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public class SchemaConverter {

    private static final Logger LOG = LoggerFactory.getLogger(SchemaConverter.class);

    public static org.apache.beam.sdk.schemas.Schema getSchema(org.apache.avro.Schema avroSchema) {
        org.apache.beam.sdk.schemas.Schema.Builder schemaBuilder = org.apache.beam.sdk.schemas.Schema.builder();
        avroSchema.getFields().forEach(field -> {
            schemaBuilder.addNullableField(field.name(), Mapper.AvroSchemaMapper.getSchemaTypeFromAvro(field.schema().getTypes().get(0).getType()));
        });
        return schemaBuilder.build();
    }

    public static Schema getAvroSchemaFromResource() {
        ClassLoader classLoader = SchemaConverter.class.getClassLoader();
        try {
            return new Schema.Parser().parse(new File(Objects.requireNonNull(classLoader.getResource("schema.avsc")).getFile()));
        } catch (IOException e) {
            LOG.error(e.getMessage(), e);
            return null;
        }
    }

    public static Schema getAvroSchemaFromGsp() {
        return new Schema.Parser().parse(Objects.requireNonNull(DownloadGcpObject.downloadFileAsString()));
    }

    public static String getStringAvroSchemaFromResource() {
        ClassLoader classLoader = SchemaConverter.class.getClassLoader();
        try {
            return new Schema.Parser().parse(new File(Objects.requireNonNull(classLoader.getResource("schema.avsc")).getFile())).toString();
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
                                .setType(Mapper.BigQuerySchemaMapper.getBigQueryTypes(field.getType()))
                                .setMode("NULLABLE"))
        );
        return new TableSchema().setFields(tableFieldSchemas);
    }

    public static TableSchema getTableSchema(Schema schema) {
        List<TableFieldSchema> tableFieldSchemas = new ArrayList<>();
        schema.getFields().forEach(field ->
                tableFieldSchemas.add(
                        new TableFieldSchema()
                                .setName(field.name())
                                .setType(Mapper.BigQueryAvroMapper.getBigQueryTypes(field.schema().getTypes().get(0).getType()))
                                .setMode("NULLABLE"))
        );
        return new TableSchema().setFields(tableFieldSchemas);
    }

    public static List<TableFieldSchema> getTableFields(org.apache.beam.sdk.schemas.Schema schema) {
        List<TableFieldSchema> tableFieldSchemas = new ArrayList<>();
        schema.getFields().forEach(field ->
                tableFieldSchemas.add(
                        new TableFieldSchema()
                                .setName(field.getName())
                                .setType(Mapper.BigQuerySchemaMapper.getBigQueryTypes(field.getType()))
                                .setMode("NULLABLE"))
        );
        return tableFieldSchemas;
    }

    public static List<TableFieldSchema> getTableFields(Map<String, org.apache.beam.sdk.schemas.Schema.FieldType> fieldsMap) {
        List<TableFieldSchema> tableFieldSchemas = new ArrayList<>();
        for (String fieldName: fieldsMap.keySet()
             ) {
            tableFieldSchemas.add(
                    new TableFieldSchema()
                            .setName(fieldName)
                            .setType(Mapper.BigQuerySchemaMapper.getBigQueryTypes(fieldsMap.get(fieldName)))
                            .setMode("NULLABLE"));
        }
        return tableFieldSchemas;
    }

    public static TableRow convertRowToTableRow(Row row, TableSchema tableSchema) {
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

    public static TableRow convertRowToTableRow(GenericRecord row, TableSchema tableSchema) {
        List<TableFieldSchema> fields = tableSchema.getFields();
        TableRow tableRow = new TableRow();
        for (TableFieldSchema subSchema : fields) {
            Object field = row.get(subSchema.getName());
            if (field == null) {
                continue;
            }
            tableRow.set(subSchema.getName(), field);
        }
        return tableRow;
    }

    public static TableRow convertRowToTableRow(String jsonString, TableSchema tableSchema) {
        JSONObject json = new JSONObject(jsonString);
        List<TableFieldSchema> fields = tableSchema.getFields();
        TableRow tableRow = new TableRow();
        for (TableFieldSchema subSchema : fields) {
            String fieldName = subSchema.getName();
            if (!json.keySet().contains(fieldName)) {
                tableRow.set(fieldName, null);
            } else {
                tableRow.set(fieldName, json.get(fieldName));
            }
        }
        return tableRow;
    }

}
