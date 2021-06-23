package com.github.tatisled.common.util;

import com.google.common.collect.ImmutableMap;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableMultimap;

import java.sql.Types;

public class Mapper {

    public static class BigQuerySchemaMapper {
        private BigQuerySchemaMapper() {
        }

        private static final ImmutableMultimap<String, Schema.FieldType> BIG_QUERY_TO_SCHEMA_TYPES =
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

        private static final ImmutableMultimap<org.apache.avro.Schema.Type, org.apache.beam.sdk.schemas.Schema.FieldType> AVRO_TO_SCHEMA_TYPES =
                ImmutableMultimap.<org.apache.avro.Schema.Type, org.apache.beam.sdk.schemas.Schema.FieldType>builder()
                        .put(org.apache.avro.Schema.Type.STRING, org.apache.beam.sdk.schemas.Schema.FieldType.STRING)
                        .put(org.apache.avro.Schema.Type.BYTES, org.apache.beam.sdk.schemas.Schema.FieldType.BYTES)
                        .put(org.apache.avro.Schema.Type.INT, org.apache.beam.sdk.schemas.Schema.FieldType.INT32)
                        .put(org.apache.avro.Schema.Type.FLOAT, org.apache.beam.sdk.schemas.Schema.FieldType.FLOAT)
                        .put(org.apache.avro.Schema.Type.DOUBLE, org.apache.beam.sdk.schemas.Schema.FieldType.DOUBLE)
                        .put(org.apache.avro.Schema.Type.BOOLEAN, org.apache.beam.sdk.schemas.Schema.FieldType.BOOLEAN)
                        .put(org.apache.avro.Schema.Type.LONG, org.apache.beam.sdk.schemas.Schema.FieldType.INT64)
                        .put(org.apache.avro.Schema.Type.RECORD, org.apache.beam.sdk.schemas.Schema.FieldType.STRING)
                        .build();

        private static final ImmutableMultimap<org.apache.beam.sdk.schemas.Schema.FieldType, org.apache.avro.Schema.Type> SCHEMA_TYPES_TO_AVRO =
                AVRO_TO_SCHEMA_TYPES.inverse();

        public static org.apache.avro.Schema.Type getAvroTypeFromSchema(org.apache.beam.sdk.schemas.Schema.FieldType type) {
            return SCHEMA_TYPES_TO_AVRO.get(type).iterator().next();
        }

        public static org.apache.beam.sdk.schemas.Schema.FieldType getSchemaTypeFromAvro(org.apache.avro.Schema.Type type) {
            return AVRO_TO_SCHEMA_TYPES.get(type).iterator().next();
        }
    }

    public static class AvroSQLMapper {
        private AvroSQLMapper() {
        }

        public static final ImmutableMap<org.apache.avro.Schema.Type, SqlTypeWithName> AVRO_TO_SQL_TYPES = ImmutableMap.<org.apache.avro.Schema.Type, SqlTypeWithName>builder()
                .put(org.apache.avro.Schema.Type.LONG, new SqlTypeWithName(Types.BIGINT, "BIGINT"))
                .put(org.apache.avro.Schema.Type.STRING, new SqlTypeWithName(Types.VARCHAR, "VARCHAR"))
                .put(org.apache.avro.Schema.Type.INT, new SqlTypeWithName(Types.INTEGER, "INTEGER"))
                .put(org.apache.avro.Schema.Type.DOUBLE, new SqlTypeWithName(Types.DOUBLE, "DOUBLE PRECISION"))
                .build();

        public static class SqlTypeWithName {
            int typeCode;
            String typeName;

            SqlTypeWithName(int type, String name) {
                this.typeCode = type;
                this.typeName = name;
            }

            public int getTypeCode() {
                return typeCode;
            }

            public String getTypeName() {
                return typeName;
            }

        }
    }

    public static class BigQueryAvroMapper {
        private BigQueryAvroMapper() {
        }

        private static final ImmutableMultimap<String, org.apache.avro.Schema.Type> BIG_QUERY_TO_AVRO_TYPES =
                ImmutableMultimap.<String, org.apache.avro.Schema.Type>builder()
                        .put("STRING", org.apache.avro.Schema.Type.STRING)
                        .put("GEOGRAPHY", org.apache.avro.Schema.Type.STRING)
                        .put("BYTES", org.apache.avro.Schema.Type.BYTES)
                        .put("INTEGER", org.apache.avro.Schema.Type.INT)
                        .put("FLOAT", org.apache.avro.Schema.Type.FLOAT)
                        .put("FLOAT64", org.apache.avro.Schema.Type.DOUBLE)
                        .put("NUMERIC", org.apache.avro.Schema.Type.BYTES)
                        .put("BOOLEAN", org.apache.avro.Schema.Type.BOOLEAN)
                        .put("INT64", org.apache.avro.Schema.Type.LONG)
                        .put("TIMESTAMP", org.apache.avro.Schema.Type.LONG)
                        .put("RECORD", org.apache.avro.Schema.Type.STRING)
                        .put("DATE", org.apache.avro.Schema.Type.INT)
                        .put("DATETIME", org.apache.avro.Schema.Type.STRING)
                        .put("TIME", org.apache.avro.Schema.Type.LONG)
                        .put("STRUCT", org.apache.avro.Schema.Type.RECORD)
                        .put("ARRAY", org.apache.avro.Schema.Type.ARRAY)
                        .build();

        private static final ImmutableMultimap<org.apache.avro.Schema.Type, String> AVRO_TYPES_TO_BIG_QUERY =
                BIG_QUERY_TO_AVRO_TYPES.inverse();

        public static String getBigQueryTypes(org.apache.avro.Schema.Type type) {
            return AVRO_TYPES_TO_BIG_QUERY.get(type).iterator().next();
        }
    }

}
