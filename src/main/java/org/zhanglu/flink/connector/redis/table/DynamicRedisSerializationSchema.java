package org.zhanglu.flink.connector.redis.table;

import org.zhanglu.flink.connector.redis.RedisRecord;
import org.zhanglu.flink.connector.redis.RedisSerializationSchema;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.types.RowKind;

import java.util.HashMap;
import java.util.Map;

/**
 * Redis序列化
 *
 * @author zhanglu
 * @date 2021/11/25 20:4
 */
class DynamicRedisSerializationSchema implements RedisSerializationSchema<RowData> {
    private static final int INT_CAP = 16;

    private static final long serialVersionUID = 1L;

    private final SerializationSchema<RowData> keySerialization;

    private final RowData.FieldGetter[] keyFieldGetters;

    private final RowData.FieldGetter[] valueFieldGetters;

    private final String[] fieldNames;

    DynamicRedisSerializationSchema(
            SerializationSchema<RowData> keySerialization,
            RowData.FieldGetter[] keyFieldGetters,
            RowData.FieldGetter[] valueFieldGetters,
            String[] fieldNames) {
        this.keySerialization = keySerialization;
        this.keyFieldGetters = keyFieldGetters;
        this.valueFieldGetters = valueFieldGetters;
        this.fieldNames = fieldNames;
    }

    @Override
    public void open(SerializationSchema.InitializationContext context) throws Exception {
        keySerialization.open(context);
    }

    @Override
    public RedisRecord<byte[], Map<String, Object>> serialize(RowData consumedRow) {
        final RowData keyRow = createProjectedRow(consumedRow, RowKind.INSERT, keyFieldGetters);
        final byte[] keySerialized = keySerialization.serialize(keyRow);

        final Map<String, Object> valueMap = new HashMap<>(INT_CAP);
        final int arity = valueFieldGetters.length;
        for (int fieldPos = 0; fieldPos < arity; fieldPos++) {
            Object value = valueFieldGetters[fieldPos].getFieldOrNull(consumedRow);
            valueMap.put(fieldNames[fieldPos], value);
        }

        return new RedisRecord<>(
            consumedRow.getRowKind(),
            keySerialized,
            valueMap);
    }

    private static RowData createProjectedRow(
            RowData consumedRow, RowKind kind, RowData.FieldGetter[] fieldGetters) {
        final int arity = fieldGetters.length;
        final GenericRowData genericRowData = new GenericRowData(kind, arity);
        for (int fieldPos = 0; fieldPos < arity; fieldPos++) {
            genericRowData.setField(fieldPos, fieldGetters[fieldPos].getFieldOrNull(consumedRow));
        }
        return genericRowData;
    }
}
