package com.example.schema;

import org.apache.avro.Schema;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

public class DebeziumAvroToIcebergSchemaConverter {

    public static org.apache.iceberg.Schema convert(Schema avroEnvelopeSchema) {
        // 1. extract "after" field from envelope
        Schema afterSchema = unwrapUnion(avroEnvelopeSchema.getField("after").schema());

        // 2. convert each field
        AtomicInteger fieldId = new AtomicInteger(1);
        List<Types.NestedField> fields = new ArrayList<>();

        for (Schema.Field field : afterSchema.getFields()) {
            Types.NestedField icebergField = convertField(field, fieldId);
            fields.add(icebergField);
        }

        return new org.apache.iceberg.Schema(fields);
    }

    private static Types.NestedField convertField(Schema.Field field, AtomicInteger fieldId) {
        int id = fieldId.getAndIncrement();
        String name = field.name();

        Schema fieldSchema = unwrapUnion(field.schema());
        boolean isNullable = isNullable(field.schema());

        Type icebergType = avroTypeToIceberg(fieldSchema, fieldId);

        if (isNullable) {
            return Types.NestedField.optional(id, name, icebergType);
        } else {
            return Types.NestedField.required(id, name, icebergType);
        }
    }

    private static Type avroTypeToIceberg(Schema schema, AtomicInteger fieldId) {
        // handle Debezium / Kafka Connect logical types first
        String logicalType = schema.getProp("logicalType");
        String connectName = schema.getProp("connect.name");

        // org.apache.kafka.connect.data.Decimal → DECIMAL
        if ("decimal".equals(logicalType) ||
                "org.apache.kafka.connect.data.Decimal".equals(connectName)) {
            int precision = Integer.parseInt(schema.getProp("precision") != null
                    ? schema.getProp("precision")
                    : schema.getObjectProp("precision").toString());
            int scale = Integer.parseInt(schema.getProp("scale") != null
                    ? schema.getProp("scale")
                    : schema.getObjectProp("scale").toString());
            return Types.DecimalType.of(precision, scale);
        }

        // io.debezium.time.Date → DATE (int epoch days)
        if ("io.debezium.time.Date".equals(connectName)) {
            return Types.DateType.get();
        }

        // io.debezium.time.MicroTimestamp → TIMESTAMP (long microseconds)
        if ("io.debezium.time.MicroTimestamp".equals(connectName)) {
            return Types.TimestampType.withoutZone();
        }

        // io.debezium.time.Timestamp → TIMESTAMP (long milliseconds)
        if ("io.debezium.time.Timestamp".equals(connectName)) {
            return Types.TimestampType.withoutZone();
        }

        // io.debezium.time.ZonedTimestamp → TIMESTAMPTZ
        if ("io.debezium.time.ZonedTimestamp".equals(connectName)) {
            return Types.TimestampType.withZone();
        }

        // primitive types
        switch (schema.getType()) {
            case INT:    return Types.IntegerType.get();
            case LONG:   return Types.LongType.get();
            case FLOAT:  return Types.FloatType.get();
            case DOUBLE: return Types.DoubleType.get();
            case BOOLEAN: return Types.BooleanType.get();
            case STRING: return Types.StringType.get();
            case BYTES:
            case FIXED:  return Types.BinaryType.get();
            case RECORD: {
                List<Types.NestedField> fields = new ArrayList<>();
                for (Schema.Field f : schema.getFields()) {
                    fields.add(convertField(f, fieldId));
                }
                return Types.StructType.of(fields);
            }
            case ARRAY:
                return Types.ListType.ofOptional(
                        fieldId.getAndIncrement(),
                        avroTypeToIceberg(schema.getElementType(), fieldId)
                );
            case MAP:
                return Types.MapType.ofOptional(
                        fieldId.getAndIncrement(),
                        fieldId.getAndIncrement(),
                        Types.StringType.get(),
                        avroTypeToIceberg(schema.getValueType(), fieldId)
                );
            default:
                throw new UnsupportedOperationException("Unsupported Avro type: " + schema.getType());
        }
    }

    // unwrap ["null", actualType] or [actualType, "null"] unions
    private static Schema unwrapUnion(Schema schema) {
        if (schema.getType() != Schema.Type.UNION) return schema;
        return schema.getTypes().stream()
                .filter(s -> s.getType() != Schema.Type.NULL)
                .findFirst()
                .orElseThrow(() -> new IllegalStateException("Union has no non-null type: " + schema));
    }

    private static boolean isNullable(Schema schema) {
        if (schema.getType() != Schema.Type.UNION) return false;
        return schema.getTypes().stream().anyMatch(s -> s.getType() == Schema.Type.NULL);
    }
}
