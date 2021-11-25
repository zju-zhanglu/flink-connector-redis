package org.zhanglu.flink.connector.redis.table;

import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.format.EncodingFormat;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.sink.SinkFunctionProvider;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.utils.DataTypeUtils;
import org.apache.flink.util.Preconditions;
import org.zhanglu.flink.connector.redis.FlinkRedisProducer;

import javax.annotation.Nullable;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

import static org.zhanglu.flink.connector.redis.table.RedisOption.ServerOption;

/**
 * 自定义的RedisSink
 *
 * @author zhanglu
 * @date 2021/11/25 20:4
 */
public class RedisDynamicTableSink implements DynamicTableSink {
    /** Data type to configure the formats. */
    protected final DataType physicalDataType;
    /** Optional format for encoding keys to Kafka. */
    protected final EncodingFormat<SerializationSchema<RowData>> keyEncodingFormat;
    /** Indices that determine the key fields and the source position in the consumed row. */
    protected final int[] keyProjection;
    /** Indices that determine the value fields and the source position in the consumed row. */
    protected final int[] valueProjection;
    protected final RedisOption.Command command;
    protected final ServerOption serverOption;
    protected final String tableName;
    protected final @Nullable String keyPrefix;
    protected final @Nullable Integer ttl;
    protected final @Nullable Integer bufferExpire;
    protected final @Nullable List<String> bufferExcludeFields;
    protected final @Nullable String script;
    protected final @Nullable Integer parallelism;

    public RedisDynamicTableSink(
        DataType physicalDataType,
        EncodingFormat<SerializationSchema<RowData>> keyEncodingFormat,
        int[] keyProjection,
        int[] valueProjection,
        RedisOption.Command command,
        ServerOption serverOption,
        String tableName,
        @Nullable String keyPrefix,
        @Nullable Integer ttl,
        @Nullable Integer bufferExpire,
        @Nullable List<String> bufferExcludeFields,
        @Nullable String script,
        @Nullable Integer parallelism) {
        this.physicalDataType =
                Preconditions.checkNotNull(
                        physicalDataType, "Physical data type must not be null.");
        this.keyEncodingFormat = Preconditions.checkNotNull(
                keyEncodingFormat, "Key encoding format must not be null.");
        this.keyProjection =
                Preconditions.checkNotNull(keyProjection, "Key projection must not be null.");
        this.valueProjection =
                Preconditions.checkNotNull(valueProjection, "Value projection must not be null.");
        this.command = command;
        this.serverOption = serverOption;
        this.tableName = tableName;
        this.keyPrefix = keyPrefix;
        this.ttl = ttl;
        this.bufferExpire = bufferExpire;
        this.bufferExcludeFields = bufferExcludeFields;
        this.script = script;
        this.parallelism = parallelism;
    }

    @Override
    public ChangelogMode getChangelogMode(ChangelogMode changelogMode) {
        return changelogMode;
    }

    @Override
    public SinkRuntimeProvider getSinkRuntimeProvider(Context context) {
        final SerializationSchema<RowData> keySerialization =
                createSerialization(context, keyEncodingFormat, keyProjection);

        final FlinkRedisProducer<RowData> redisProducer =
                createRedisProducer(keySerialization);

        return SinkFunctionProvider.of(redisProducer, parallelism);
    }

    @Override
    public DynamicTableSink copy() {
        return new RedisDynamicTableSink(
            physicalDataType,
            keyEncodingFormat,
            keyProjection,
            valueProjection,
            command,
            serverOption,
            tableName,
            keyPrefix,
            ttl,
            bufferExpire,
            bufferExcludeFields,
            script,
            parallelism);
    }

    @Override
    public String asSummaryString() {
        return "Redis table sink";
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        final RedisDynamicTableSink that = (RedisDynamicTableSink) o;
        return Objects.equals(physicalDataType, that.physicalDataType)
            && Objects.equals(keyEncodingFormat, that.keyEncodingFormat)
            && Arrays.equals(keyProjection, that.keyProjection)
            && Arrays.equals(valueProjection, that.valueProjection)
            && Objects.equals(command, that.command)
            && Objects.equals(serverOption, that.serverOption)
            && Objects.equals(tableName, that.tableName)
            && Objects.equals(keyPrefix, that.keyPrefix)
            && Objects.equals(ttl, that.ttl)
            && Objects.equals(bufferExpire, that.bufferExpire)
            && Objects.equals(script, that.script)
            && Objects.equals(parallelism, that.parallelism);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
            physicalDataType,
            keyEncodingFormat,
            keyProjection,
            valueProjection,
            command,
            serverOption,
            tableName,
            keyPrefix,
            ttl,
            bufferExpire,
            bufferExcludeFields,
            script,
            parallelism);
    }

    // --------------------------------------------------------------------------------------------

    public FlinkRedisProducer<RowData> createRedisProducer(SerializationSchema<RowData> keySerialization) {
        final List<LogicalType> physicalChildren = physicalDataType.getLogicalType().getChildren();
        final RowData.FieldGetter[] keyFieldGetters =
            Arrays.stream(keyProjection)
                .mapToObj(targetField -> RowData.createFieldGetter(
                    physicalChildren.get(targetField), targetField))
                .toArray(RowData.FieldGetter[]::new);

        final RowData.FieldGetter[] valueFieldGetters =
            Arrays.stream(valueProjection)
                .mapToObj(targetField -> RowData.createFieldGetter(
                    physicalChildren.get(targetField), targetField))
                .toArray(RowData.FieldGetter[]::new);

        DataType valueDataType = DataTypeUtils.projectRow(physicalDataType, valueProjection);
        RowType rowType = (RowType)valueDataType.getLogicalType();
        final String[] fieldNames = rowType.getFieldNames().toArray(new String[0]);

        final DynamicRedisSerializationSchema redisSerializer =
            new DynamicRedisSerializationSchema(
                keySerialization,
                keyFieldGetters,
                valueFieldGetters,
                fieldNames);

        return new FlinkRedisProducer<>(
            serverOption,
            redisSerializer,
            command,
            tableName,
            keyPrefix,
            ttl,
            bufferExpire,
            bufferExcludeFields,
            script
        );
    }

    private @Nullable SerializationSchema<RowData> createSerialization(
            DynamicTableSink.Context context,
            @Nullable EncodingFormat<SerializationSchema<RowData>> format,
            int[] projection) {
        if (format == null) {
            return null;
        }
        DataType physicalFormatDataType =
            DataTypeUtils.projectRow(this.physicalDataType, projection);
        return format.createRuntimeEncoder(context, physicalFormatDataType);
    }
}
