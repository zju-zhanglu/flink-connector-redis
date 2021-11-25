package org.zhanglu.flink.connector.redis.table;

import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.catalog.ResolvedCatalogTable;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.catalog.UniqueConstraint;
import org.apache.flink.table.connector.format.EncodingFormat;
import org.apache.flink.table.connector.format.Format;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.factories.DynamicTableSinkFactory;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.factories.SerializationFormatFactory;
import org.apache.flink.table.types.DataType;
import org.apache.flink.types.RowKind;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.apache.flink.table.factories.FactoryUtil.SINK_PARALLELISM;
import static org.zhanglu.flink.connector.redis.table.RedisOption.BUFFER_EXPIRE;
import static org.zhanglu.flink.connector.redis.table.RedisOption.BUFFER_FIELDS_EXCLUDE;
import static org.zhanglu.flink.connector.redis.table.RedisOption.COMMAND;
import static org.zhanglu.flink.connector.redis.table.RedisOption.DATABASE;
import static org.zhanglu.flink.connector.redis.table.RedisOption.HOST;
import static org.zhanglu.flink.connector.redis.table.RedisOption.KEY_FIELDS_PREFIX;
import static org.zhanglu.flink.connector.redis.table.RedisOption.KEY_FORMAT;
import static org.zhanglu.flink.connector.redis.table.RedisOption.MASTER_NAME;
import static org.zhanglu.flink.connector.redis.table.RedisOption.NODES;
import static org.zhanglu.flink.connector.redis.table.RedisOption.PASSWORD;
import static org.zhanglu.flink.connector.redis.table.RedisOption.PORT;
import static org.zhanglu.flink.connector.redis.table.RedisOption.SCRIPT;
import static org.zhanglu.flink.connector.redis.table.RedisOption.SENTINELS;
import static org.zhanglu.flink.connector.redis.table.RedisOption.SERVER_MODE;
import static org.zhanglu.flink.connector.redis.table.RedisOption.ServerOption;
import static org.zhanglu.flink.connector.redis.table.RedisOption.TABLE_ALIAS;
import static org.zhanglu.flink.connector.redis.table.RedisOption.TTL;
import static org.zhanglu.flink.connector.redis.table.RedisOption.VALUE_FIELDS_INCLUDE;
import static org.zhanglu.flink.connector.redis.table.RedisOption.createKeyFormatProjection;
import static org.zhanglu.flink.connector.redis.table.RedisOption.createValueFormatProjection;
import static org.zhanglu.flink.connector.redis.table.RedisOption.getServerOptions;
import static org.zhanglu.flink.connector.redis.table.RedisOption.validateTableSinkOptions;

/**
 * 生成自定义redisSink的工厂类
 *
 * @author zhanglu
 * @date 2021/11/25 20:5
 */
public class RedisDynamicTableSinkFactory implements DynamicTableSinkFactory {

    public static final String IDENTIFIER = "redis";

    @Override
    public DynamicTableSink createDynamicTableSink(Context context) {
        final FactoryUtil.TableFactoryHelper helper = FactoryUtil.createTableFactoryHelper(this, context);
        final ReadableConfig tableOptions = helper.getOptions();
        final EncodingFormat<SerializationSchema<RowData>> keyEncodingFormat =
            helper.discoverEncodingFormat(SerializationFormatFactory.class, KEY_FORMAT);

        helper.validate();
        ResolvedSchema schema = context.getCatalogTable().getResolvedSchema();
        validateSink(tableOptions, keyEncodingFormat, schema);

        final ServerOption serverOption = getServerOptions(tableOptions);

        Tuple2<int[], int[]> keyValueProjections =
            createKeyValueProjections(context.getCatalogTable());

        String tableName = context.getObjectIdentifier().getObjectName();
        tableName = tableOptions.getOptional(TABLE_ALIAS).orElse(tableName);
        final String keyPrefix = tableOptions.getOptional(KEY_FIELDS_PREFIX).orElse("");


        final RedisOption.Command command = tableOptions.get(COMMAND);
        final Integer ttl = tableOptions.getOptional(TTL).orElse(0);
        final Integer bufferExpire = tableOptions.getOptional(BUFFER_EXPIRE).orElse(0);
        final List<String> bufferExcludeFields = tableOptions.getOptional(BUFFER_FIELDS_EXCLUDE)
            .orElse(Collections.emptyList());

        final String script = tableOptions.getOptional(SCRIPT).orElse(null);
        final Integer parallelism = tableOptions.getOptional(SINK_PARALLELISM).orElse(null);

        return new RedisDynamicTableSink(
            schema.toPhysicalRowDataType(),
            keyEncodingFormat,
            keyValueProjections.f0,
            keyValueProjections.f1,
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

    private Tuple2<int[], int[]> createKeyValueProjections(ResolvedCatalogTable catalogTable) {
        ResolvedSchema schema = catalogTable.getResolvedSchema();
        List<String> keyFields = schema.getPrimaryKey().map(UniqueConstraint::getColumns).orElse(new ArrayList<>());
        DataType physicalDataType = schema.toPhysicalRowDataType();

        Configuration tableOptions = Configuration.fromMap(catalogTable.getOptions());

        int[] keyProjection = createKeyFormatProjection(keyFields, physicalDataType);
        int[] valueProjection = createValueFormatProjection(tableOptions, keyFields, physicalDataType);

        return Tuple2.of(keyProjection, valueProjection);
    }

    private static void validateSink(
            ReadableConfig tableOptions, Format keyFormat, ResolvedSchema schema) {
        validateFormat(keyFormat, tableOptions);
        validateTableSinkOptions(tableOptions);
    }

    private static void validateFormat(
            Format keyFormat, ReadableConfig tableOptions) {
        if (!keyFormat.getChangelogMode().containsOnly(RowKind.INSERT)) {
            String identifier = tableOptions.get(KEY_FORMAT);
            throw new ValidationException(
                String.format(
                    "'redis' connector doesn't support '%s' as key format, "
                        + "because '%s' is not in insert-only mode.",
                    identifier, identifier));
        }
    }

    @Override
    public String factoryIdentifier() {
        return IDENTIFIER;
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        Set<ConfigOption<?>> options = new HashSet<>();
        options.add(SERVER_MODE);
        return options;
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        Set<ConfigOption<?>> options = new HashSet<>();
        options.add(HOST);
        options.add(PORT);
        options.add(PASSWORD);
        options.add(DATABASE);
        options.add(MASTER_NAME);
        options.add(SENTINELS);
        options.add(NODES);
        options.add(KEY_FORMAT);
        options.add(KEY_FIELDS_PREFIX);
        options.add(VALUE_FIELDS_INCLUDE);
        options.add(TTL);
        options.add(BUFFER_EXPIRE);
        options.add(BUFFER_FIELDS_EXCLUDE);
        options.add(TABLE_ALIAS);
        options.add(FactoryUtil.SINK_PARALLELISM);
        options.add(COMMAND);
        options.add(SCRIPT);
        return options;
    }
}
