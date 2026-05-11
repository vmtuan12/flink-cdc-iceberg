package com.example.deserialization;

import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DecoderFactory;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.data.*;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.Collector;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.List;

import com.example.schema.SchemaUtilsV2;

public class DebeziumAvroToRowDataDeserializer
        implements KafkaRecordDeserializationSchema<RowData> {

    private static final int MAGIC_BYTE = 0x00;
    private static final int ID_SIZE = 4;
    private static final int PREFIX_SIZE = 1 + ID_SIZE;

    private final String schemaRegistryUrl;
    private transient SchemaRegistryClient registryClient;
    private static final Logger LOG = LoggerFactory.getLogger(DebeziumAvroToRowDataDeserializer.class);

    public DebeziumAvroToRowDataDeserializer(String schemaRegistryUrl) {
        this.schemaRegistryUrl = schemaRegistryUrl;
    }

    // lazy init (important for Flink serialization)
    private SchemaRegistryClient getClient() {
        if (registryClient == null) {
            registryClient = new CachedSchemaRegistryClient(schemaRegistryUrl, 1000);
        }
        return registryClient;
    }

    @Override
    public void deserialize(ConsumerRecord<byte[], byte[]> record,
                            Collector<RowData> out) throws IOException {

        byte[] valueBytes = record.value();
        if (valueBytes == null) {
            return; // tombstone
        }

        // validate magic byte
        if (valueBytes[0] != MAGIC_BYTE) {
            throw new IOException("Invalid Confluent Avro message (missing magic byte)");
        }

        int schemaId = ByteBuffer.wrap(valueBytes, 1, ID_SIZE).getInt();
        GenericRecord envelope = decodeAvro(valueBytes, schemaId);

        GenericRecord before = (GenericRecord) envelope.get("before");
        GenericRecord after  = (GenericRecord) envelope.get("after");
        String op = envelope.get("op").toString();

//        Schema rowSchema = resolveRowSchema(envelope.getSchema(), "after");
//        List<Schema.Field> fields = rowSchema.getFields();
//        for (Schema.Field f : fields) {
//            Schema realType = unwrapNullableUnion(f.schema());
//            System.out.println(f.name() + " ### value: " + after.get(f.name()) + " ### " + realType.toString() + " --- class: " + realType.getClass().toString() + " --- type: " + realType.getType());
//            System.out.println(realType.getType() == Schema.Type.STRING);
//            if (realType.getLogicalType() != null) {
//                System.out.println(realType.getObjectProp("scale"));
//                System.out.println(realType.getObjectProp("precision"));
//                System.out.println("logical type: " + realType.getLogicalType().toString());
//                System.out.println(realType.getLogicalType().getClass() == LogicalTypes.Decimal.class);
//            }
//            if (realType.getObjectProp("connect.name") != null) {
//                System.out.println("connect.name: " + realType.getObjectProp("connect.name"));
//                try {
//                    Class<?> klazz = Class.forName(realType.getObjectProp("connect.name").toString());
//                    System.out.println("class: " + klazz.toString());
//                } catch (ClassNotFoundException clfe) {
//                    System.out.println("Cannot find class " + realType.getObjectProp("connect.name").toString());
//                }
//            }
//            System.out.println("---------------------------------------------------------------");
//        }

//        System.out.println("Debug rowSchema: " + rowSchema.toString());
//        System.out.println("Debug after: type " + envelope.get("after").getClass().toString() + " --- " + envelope.get("after"));

        System.out.println("Op: " + op);
        switch (op) {
            case "c", "r":
                out.collect(recordToRowData(after, RowKind.INSERT));
                break;
            case "u":
                if (before != null) {
                    out.collect(recordToRowData(before, RowKind.UPDATE_BEFORE));
                }
                out.collect(recordToRowData(after, RowKind.UPDATE_AFTER));
                break;
            case "d":
                out.collect(recordToRowData(before, RowKind.DELETE));
                break;
            default:
                throw new RuntimeException("Unknown Debezium op: " + op);
        }
    }

    private GenericRecord decodeAvro(byte[] bytes, int schemaId) throws IOException {
        Schema writerSchema;
        try {
            writerSchema = new Schema.Parser().parse(
                    getClient().getSchemaById(schemaId).canonicalString()
            );
        } catch (Exception e) {
            throw new IOException("Failed to fetch schema id=" + schemaId, e);
        }
//        System.out.println("Debug schema: " + writerSchema.toString());

        ByteArrayInputStream in =
                new ByteArrayInputStream(bytes, PREFIX_SIZE, bytes.length - PREFIX_SIZE);

        BinaryDecoder decoder = DecoderFactory.get().binaryDecoder(in, null);

        GenericDatumReader<GenericRecord> reader =
                new GenericDatumReader<>(writerSchema);

        return reader.read(null, decoder);
    }

    // ============================
    // 🔥 CORE: GenericRecord → RowData
    // ============================

    private GenericRowData recordToRowData(GenericRecord record, RowKind kind) {
        Schema rowSchema = SchemaUtilsV2.unwrapNullableUnion(record.getSchema());
        List<Schema.Field> fields = rowSchema.getFields();

        GenericRowData grd = new GenericRowData(kind, fields.size());
        for (int i = 0; i < fields.size(); i++) {
            Object value = record.get(fields.get(i).name());
            DataType fieldDataType = SchemaUtilsV2.getFieldClass(fields.get(i));
            grd.setField(i, convertValue(value, fieldDataType));
        }

        return grd;
    }

//    private GenericRowData convertToRowData(GenericRecord record, String op) {
//        List<Schema.Field> fields = record.getSchema().getFields();
//
//        // +1 for __op column
//        GenericRowData rowData = new GenericRowData(fields.size() + 1);
//
//        // first column = operation
//        rowData.setField(0, StringData.fromString(op));
//
//        for (int i = 0; i < fields.size(); i++) {
//            Object value = record.get(fields.get(i).name());
//            rowData.setField(i + 1, convertValue(value));
//        }
//
//        return rowData;
//    }
    private Object convertValue(Object value, DataType fieldDatatype) {
        if (value == null) return null;

        LogicalTypeRoot typeRoot = fieldDatatype.getLogicalType().getTypeRoot();
        switch (typeRoot) {
            case CHAR:
            case VARCHAR:
                return StringData.fromString(value.toString());

            case BOOLEAN:
                if (value instanceof Boolean) return value;
                return Boolean.parseBoolean(value.toString());

            case TINYINT:
                return ((Number) value).byteValue();
            case SMALLINT:
                return ((Number) value).shortValue();
            case INTEGER:
                return ((Number) value).intValue();
            case BIGINT:
                return ((Number) value).longValue();
            case FLOAT:
                return ((Number) value).floatValue();
            case DOUBLE:
                return ((Number) value).doubleValue();

            case DECIMAL: {
                org.apache.flink.table.types.logical.DecimalType dt =
                        (org.apache.flink.table.types.logical.DecimalType) fieldDatatype.getLogicalType();
                BigDecimal bd;

                if (value instanceof BigDecimal) {
                    bd = (BigDecimal) value;

                } else if (value instanceof byte[]) {
                    bd = new BigDecimal(new BigInteger((byte[]) value), dt.getScale());

                } else if (value instanceof java.nio.ByteBuffer) {
                    java.nio.ByteBuffer buf = (java.nio.ByteBuffer) value;
                    byte[] bytes = new byte[buf.remaining()];
                    buf.get(bytes);
                    if (bytes.length == 0) {
                        bd = BigDecimal.ZERO;  // empty bytes = 0
                    } else {
                        bd = new BigDecimal(new BigInteger(bytes), dt.getScale());
                    }

                } else {
                    String s = value.toString();
                    bd = new BigDecimal(s);
                }

                return org.apache.flink.table.data.DecimalData.fromBigDecimal(
                        bd, dt.getPrecision(), dt.getScale());
            }

            case DATE: {
                if (value instanceof Integer) return value;  // already epoch-days
                // Parse ISO date string "2024-01-15"
                LocalDate ld = LocalDate.parse(value.toString(),
                        DateTimeFormatter.ISO_LOCAL_DATE);
                return (int) ld.toEpochDay();
            }

            case TIMESTAMP_WITHOUT_TIME_ZONE:
            case TIMESTAMP_WITH_TIME_ZONE:
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE: {
                if (value instanceof Integer && (Integer) value == 0) {
                    return null;
                }
                if (value instanceof Long) {
                    // Debezium sends microseconds since epoch for TIMESTAMP columns
                    return TimestampData.fromEpochMillis(
                            ((Long) value) / 1_000,
                            (int) (((Long) value) % 1_000) * 1_000);
                }
                LocalDateTime ldt = LocalDateTime.parse(
                        value.toString(), DateTimeFormatter.ISO_LOCAL_DATE_TIME);
                return TimestampData.fromLocalDateTime(ldt);
            }

            case BINARY:
            case VARBINARY:
                if (value instanceof byte[]) return value;
                return value.toString().getBytes(java.nio.charset.StandardCharsets.UTF_8);

            default:
                LOG.warn("Unhandled type {}; falling back to string", typeRoot);
                return StringData.fromString(value.toString());
        }
    }

//    private Object convertValue(Object value) {
//        if (value == null) return null;
//
//        System.out.println("value: " + value + " --- type: " + value.getClass().toString());
//
//        // string
//        if (value instanceof CharSequence) {
//            return StringData.fromString(value.toString());
//        }
//
//        // primitive types
//        if (value instanceof Integer ||
//                value instanceof Long ||
//                value instanceof Double ||
//                value instanceof Float ||
//                value instanceof Boolean) {
//            return value;
//        }
//
//        // bytes
//        if (value instanceof ByteBuffer buffer) {
//            return buffer.array();
//        }
//
//        // decimal (Debezium often uses bytes or string)
//        if (value instanceof byte[] bytes) {
//            try {
//                return DecimalData.fromBigDecimal(
//                        new BigDecimal(new String(bytes)),
//                        38,
//                        18
//                );
//            } catch (Exception e) {
//                return bytes; // fallback
//            }
//        }
//
//        // nested struct
//        if (value instanceof GenericRecord nested) {
//            return convertToRowData(nested, "nested");
//        }
//
//        throw new RuntimeException("Unsupported type: " + value.getClass());
//    }

    @Override
    public TypeInformation<RowData> getProducedType() {
        return TypeInformation.of(RowData.class);
    }
}