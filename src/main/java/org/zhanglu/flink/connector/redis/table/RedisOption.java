package org.zhanglu.flink.connector.redis.table;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.logical.utils.LogicalTypeChecks;
import org.apache.flink.util.Preconditions;

import java.io.Serializable;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.apache.flink.table.factories.FactoryUtil.FORMAT_SUFFIX;
import static org.apache.flink.table.types.logical.utils.LogicalTypeChecks.hasRoot;

/**
 * Option utils for Redis table source sink
 *
 * @author zhanglu
 * @date 2021/5/4 21:6
 */
public class RedisOption {
    /**
     * Option enumerations
     */
    public static final String SERVER_MODE_STANDALONE = "standalone";
    public static final String SERVER_MODE_SENTINEL = "sentinel";
    public static final String SERVER_MODE_CLUSTER = "cluster";
    private static final Set<String> SERVER_MODE_ENUMS =
        new HashSet<>(Arrays.asList(
            SERVER_MODE_STANDALONE,
            SERVER_MODE_SENTINEL,
            SERVER_MODE_CLUSTER
        ));
    public static final ConfigOption<String> SERVER_MODE = ConfigOptions.key("serverMode")
        .stringType()
        .defaultValue(SERVER_MODE_STANDALONE);
    public static final ConfigOption<String> HOST = ConfigOptions.key("host").stringType().noDefaultValue();
    public static final ConfigOption<Integer> PORT = ConfigOptions.key("port").intType().defaultValue(6379);
    public static final ConfigOption<String> PASSWORD = ConfigOptions.key("password").stringType().noDefaultValue();
    public static final ConfigOption<Integer> DATABASE = ConfigOptions.key("database").intType().defaultValue(0);
    public static final ConfigOption<String> MASTER_NAME = ConfigOptions.key("masterName")
        .stringType()
        .noDefaultValue();
    public static final ConfigOption<List<String>> SENTINELS = ConfigOptions.key("sentinels")
        .stringType()
        .asList()
        .noDefaultValue();
    public static final ConfigOption<List<String>> NODES = ConfigOptions.key("nodes")
        .stringType()
        .asList()
        .noDefaultValue();
    public static final ConfigOption<String> KEY_FORMAT = ConfigOptions.key("key" + FORMAT_SUFFIX)
        .stringType()
        .noDefaultValue();
    public static final ConfigOption<String> KEY_FIELDS_PREFIX = ConfigOptions.key("key.fields-prefix")
        .stringType()
        .noDefaultValue();
    public static final ConfigOption<ValueFieldsStrategy> VALUE_FIELDS_INCLUDE =
        ConfigOptions.key("value.fields-include")
            .enumType(ValueFieldsStrategy.class)
            .defaultValue(ValueFieldsStrategy.ALL);
    public static final ConfigOption<Command> COMMAND = ConfigOptions.key("command")
        .enumType(Command.class)
        .defaultValue(Command.EVAL);
    public static final ConfigOption<Integer> TTL = ConfigOptions.key("ttl")
        .intType()
        .defaultValue(0);
    public static final ConfigOption<String> SCRIPT = ConfigOptions.key("script")
        .stringType()
        .noDefaultValue();
    public static final ConfigOption<String> TABLE_ALIAS = ConfigOptions.key("table-alias")
        .stringType()
        .noDefaultValue();
    public static final ConfigOption<Integer> BUFFER_EXPIRE = ConfigOptions.key("buffer.expire-ms")
        .intType()
        .defaultValue(0);
    public static final ConfigOption<List<String>> BUFFER_FIELDS_EXCLUDE = ConfigOptions
        .key("buffer.fields-exclude")
        .stringType()
        .asList()
        .noDefaultValue()
        .withDescription(
            "Exclude fields for generate buffer data. "
                + "This is applicable only when the option `buffer.expired-ms` is set to non-zero."
        );

    private RedisOption() {
    }

    public static void validateTableSinkOptions(ReadableConfig tableOptions) {
        validateServerMode(tableOptions);
        validateCommand(tableOptions);
    }

    private static void validateServerMode(ReadableConfig tableOptions) {
        tableOptions.getOptional(SERVER_MODE)
            .map(String::toLowerCase)
            .ifPresent(mode -> {
                if (!SERVER_MODE_ENUMS.contains(mode)) {
                    throw new ValidationException(
                        String.format(
                            "Invalid value for option '%s'. Supported values are %s, but was: %s",
                            SERVER_MODE.key(),
                            "[standalone, sentinel, cluster]",
                            mode));
                }
                if (SERVER_MODE_STANDALONE.equals(mode)) {
                    if (!tableOptions.getOptional(HOST).isPresent()) {
                        throw new ValidationException(
                            String.format(
                                "'%s' is required in '%s' startup mode but missing.",
                                HOST.key(),
                                SERVER_MODE_STANDALONE));
                    }
                    if (!tableOptions.getOptional(PORT).isPresent()) {
                        throw new ValidationException(
                            String.format(
                                "'%s' is required in '%s' startup mode but missing.",
                                PORT.key(),
                                SERVER_MODE_STANDALONE));
                    }
                }
                if (SERVER_MODE_SENTINEL.equals(mode)) {
                    if (!tableOptions.getOptional(MASTER_NAME).isPresent()) {
                        throw new ValidationException(
                            String.format(
                                "'%s' is required in '%s' startup mode but missing.",
                                MASTER_NAME.key(),
                                SERVER_MODE_SENTINEL));
                    }
                    if (!tableOptions.getOptional(SENTINELS).isPresent()
                        || tableOptions.getOptional(SENTINELS).get().isEmpty()) {
                        throw new ValidationException(
                            String.format(
                                "'%s' is required in '%s' startup mode but missing.",
                                SENTINELS.key(),
                                SERVER_MODE_SENTINEL));
                    }
                }
                if (SERVER_MODE_CLUSTER.equals(mode)) {
                    if (!tableOptions.getOptional(NODES).isPresent()
                        || tableOptions.getOptional(NODES).get().isEmpty()) {
                        throw new ValidationException(
                            String.format(
                                "'%s' is required in '%s' startup mode but missing.",
                                NODES.key(),
                                SERVER_MODE_CLUSTER));
                    }
                }
            });
    }

    private static void validateCommand(ReadableConfig tableOptions) {
        tableOptions.getOptional(COMMAND)
            .ifPresent(command -> {
                if (Command.EVAL.equals(command)) {
                    if (!tableOptions.getOptional(SCRIPT).isPresent()
                        || tableOptions.getOptional(SCRIPT).get().isEmpty()) {
                        throw new ValidationException(
                            String.format(
                                "'%s' is required in '%s' command but missing.",
                                SCRIPT.key(),
                                Command.EVAL));
                    }
                }
            });
    }

    /**
     * Build server options
     * @param tableOptions
     */
    public static ServerOption getServerOptions(ReadableConfig tableOptions) {
        final ServerOption options = new ServerOption();
        options.setPassword(tableOptions.getOptional(PASSWORD).orElse(null));
        options.setServerMode(tableOptions.get(SERVER_MODE));
        switch (options.getServerMode().toLowerCase()) {
            case SERVER_MODE_STANDALONE:
                options.setHost(tableOptions.get(HOST));
                options.setPort(tableOptions.get(PORT));
                options.setDatabase(tableOptions.get(DATABASE));
                break;
            case SERVER_MODE_SENTINEL:
                options.setMasterName(tableOptions.get(MASTER_NAME));
                options.setSentinels(tableOptions.get(SENTINELS));
                options.setDatabase(tableOptions.get(DATABASE));
                break;
            case SERVER_MODE_CLUSTER:
                options.setNodes(tableOptions.get(NODES));
                break;
            default:
                break;
        }
        return options;
    }

    /**
     * Creates an array of indices that determine which physical fields of the table schema to
     * include in the key format and the order that those fields have in the key format.
     */
    public static int[] createKeyFormatProjection(
            List<String> keyFields, DataType physicalDataType) {
        final LogicalType physicalType = physicalDataType.getLogicalType();
        Preconditions.checkArgument(
            hasRoot(physicalType, LogicalTypeRoot.ROW), "Row data type expected.");
        final List<String> physicalFields = LogicalTypeChecks.getFieldNames(physicalType);
        return keyFields.stream()
            .mapToInt(
                keyField -> {
                    final int pos = physicalFields.indexOf(keyField);
                    // check that field name exists
                    if (pos < 0) {
                        throw new ValidationException(
                            String.format(
                                "Could not find the field '%s' in the table schema. "
                                    + "A primary key field must be a regular, physical column. "
                                    + "The following columns can be selected in the primary key:\n"
                                    + "%s",
                                keyField, physicalFields));
                    }
                    return pos;
                })
            .toArray();
    }

    /**
     * Creates an array of indices that determine which physical fields of the table schema to
     * include in the value format.
     *
     * <p>See {@link #VALUE_FIELDS_INCLUDE}, and {@link #KEY_FIELDS_PREFIX}
     * for more information.
     */
    public static int[] createValueFormatProjection(
            ReadableConfig options, List<String> keyFields, DataType physicalDataType) {
        final LogicalType physicalType = physicalDataType.getLogicalType();
        Preconditions.checkArgument(
                hasRoot(physicalType, LogicalTypeRoot.ROW), "Row data type expected.");
        final int physicalFieldCount = LogicalTypeChecks.getFieldCount(physicalType);
        final IntStream physicalFields = IntStream.range(0, physicalFieldCount);

        final ValueFieldsStrategy strategy = options.get(VALUE_FIELDS_INCLUDE);
        if (strategy == ValueFieldsStrategy.ALL) {
            return physicalFields.toArray();
        } else if (strategy == ValueFieldsStrategy.EXCEPT_KEY) {
            final int[] keyProjection = createKeyFormatProjection(keyFields, physicalDataType);
            return physicalFields
                    .filter(pos -> IntStream.of(keyProjection).noneMatch(k -> k == pos))
                    .toArray();
        }
        throw new TableException("Unknown value fields strategy:" + strategy);
    }

    // --------------------------------------------------------------------------------------------
    // Inner classes
    // --------------------------------------------------------------------------------------------
    /** redis server options. * */
    public static class ServerOption implements Serializable {
        private String serverMode;
        private String host;
        private Integer port;
        private String password;
        private Integer database;
        private String masterName;
        private List<String> sentinels;
        private List<String> nodes;

        public String getServerMode() {
            return serverMode;
        }

        public void setServerMode(String serverMode) {
            this.serverMode = serverMode;
        }

        public Integer getPort() {
            return port;
        }

        public void setPort(Integer port) {
            this.port = port;
        }

        public String getPassword() {
            return password;
        }

        public void setPassword(String password) {
            this.password = password;
        }

        public Integer getDatabase() {
            return database;
        }

        public void setDatabase(Integer database) {
            this.database = database;
        }

        public String getMasterName() {
            return masterName;
        }

        public void setMasterName(String masterName) {
            this.masterName = masterName;
        }

        public List<String> getSentinels() {
            return sentinels;
        }

        public void setHost(String host) {
            this.host = host.startsWith("redis://") ? host : "redis://" + host;
        }

        public String getHost() {
            return host;
        }

        public void setSentinels(List<String> sentinels) {
            this.sentinels = sentinels.stream()
                .map(x -> x.startsWith("redis://") ? x : "redis://" + x)
                .collect(Collectors.toList());
        }

        public List<String> getNodes() {
            return nodes;
        }

        public void setNodes(List<String> nodes) {
            this.nodes = nodes.stream()
                .map(x -> x.startsWith("redis://") ? x : "redis://" + x)
                .collect(Collectors.toList());
        }
    }

    /**
     * Strategies to derive the data type of a value format by considering a key format
     */
    public enum ValueFieldsStrategy {
        /**
         * 包含所有字段
         */
        ALL,
        /**
         * 不包含key字段
         */
        EXCEPT_KEY
    }

    /**
     * Commands
     */
    public enum Command {
        HMSET,
        EVAL
    }
}
