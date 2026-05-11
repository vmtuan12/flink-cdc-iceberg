package com.example.schema;

import org.apache.avro.Schema;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.types.DataType;
import org.apache.iceberg.types.Types;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class SchemaUtilsV2 {
    private static final Map<Schema.Type, DataType> basicDataTypes = Map.of(
            Schema.Type.STRING, DataTypes.STRING(),
            Schema.Type.INT, DataTypes.INT(),
            Schema.Type.LONG, DataTypes.BIGINT(),
            Schema.Type.FLOAT, DataTypes.FLOAT(),
            Schema.Type.DOUBLE, DataTypes.DOUBLE(),
            Schema.Type.BOOLEAN, DataTypes.BOOLEAN()
    );

    public static Schema unwrapNullableUnion(Schema schema) {
        if (schema.getType() == Schema.Type.UNION) {
            return schema.getTypes().stream()
                    .filter(s -> s.getType() != Schema.Type.NULL)
                    .findFirst()
                    .orElseThrow(() -> new IllegalStateException(
                            "No non-null type in union: " + schema));
        }
        return schema;
    }

    public static DataType getFieldClass(Schema.Field field) {
        Schema unwarpedSchema = unwrapNullableUnion(field.schema());

        Object connectName = unwarpedSchema.getObjectProp("connect.name");
        if (connectName != null) {
            try {
                Class<?> klazz = Class.forName(connectName.toString());
                if (klazz == io.debezium.time.MicroTimestamp.class) {
                    return DataTypes.TIMESTAMP(6);
                }
                if (klazz == io.debezium.time.Date.class) {
                    return DataTypes.DATE();
                }
                if (klazz == org.apache.kafka.connect.data.Decimal.class) {
                    int precision = (Integer) unwarpedSchema.getObjectProp("precision");
                    int scale = (Integer) unwarpedSchema.getObjectProp("scale");
                    return DataTypes.DECIMAL(precision, scale);
                }
            } catch (ClassNotFoundException clfe) {
                System.out.println("Cannot find class " + connectName.toString());
            }
        }

        if (basicDataTypes.containsKey(unwarpedSchema.getType())) {
            return basicDataTypes.get(unwarpedSchema.getType());
        }

        System.out.println("Failed to determine the class of field " + field.name() + ", type " + unwarpedSchema.getType().toString());
        return null;
    }

}
