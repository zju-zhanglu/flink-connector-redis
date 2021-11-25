package org.zhanglu.flink.connector.redis;

import com.alibaba.fastjson.JSON;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.types.RowKind;
import org.redisson.Redisson;
import org.redisson.api.RMap;
import org.redisson.api.RScript;
import org.redisson.api.RedissonClient;
import org.redisson.client.RedisException;
import org.redisson.client.codec.StringCodec;
import org.redisson.config.Config;
import org.redisson.config.ReadMode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zhanglu.flink.connector.redis.table.RedisOption;

import javax.annotation.Nullable;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * @author zhanglu
 * @date 2021/5/3 23:11
 */
public class FlinkRedisProducer<T> extends RichSinkFunction<T> {
    private static final long serialVersionUID = 1L;
    private static final String NULL_VALUE = "__NULL";

    private static final Logger LOG = LoggerFactory.getLogger(FlinkRedisProducer.class);

    private final RedisOption.ServerOption serverOption;
    /**
     * (Serializable) serialization schema for serializing records to {@link RedisRecord RedisRecords}.
     */
    private final RedisSerializationSchema<T> redisSchema;
    private final RedisOption.Command command;
    private final String tableName;
    private final String keyPrefix;
    /**
     * It sets the TTL for a specific key.
     */
    private final Integer ttl;
    /**
     * It sets the expire milliseconds for a specific key in latestRecordMap.
     */
    private final Integer bufferExpire;
    /**
     * the exclude fields for generate buffer data.
     *
     * @var List
     */
    private final List<String> bufferExcludeFields;
    private final String script;

    private transient RedissonClient redisson;
    private String digest;
    private transient LinkedHashMap<Map<String, String>, Long> latestRecordMap;
    private long expireTimestamp = Long.MIN_VALUE;

    /**
     * Creates a new {@link FlinkRedisProducer} that connects to the Redis server.
     *
     * @param serverOption The configuration of redis
     * @param serializationSchema A serializable serialization schema for turning user objects into
     *     a redis-consumable byte[] supporting key/value messages
     * @param keyPrefix
     * @param ttl
     */
    public FlinkRedisProducer(
            RedisOption.ServerOption serverOption,
            RedisSerializationSchema<T> serializationSchema,
            RedisOption.Command command,
            String tableName,
            @Nullable String keyPrefix,
            @Nullable Integer ttl,
            @Nullable Integer bufferExpire,
            @Nullable List<String> bufferExcludeFields,
            @Nullable String script) {
        Objects.requireNonNull(serverOption, "ServerOption of redis should not be null");
        Objects.requireNonNull(serializationSchema, "Redis Serialization schema can not be null");
        this.serverOption = serverOption;
        this.redisSchema = serializationSchema;
        this.command = command;
        this.tableName = tableName;
        this.keyPrefix = Optional.ofNullable(keyPrefix).orElse("");
        this.ttl = Optional.ofNullable(ttl).orElse(0);
        this.bufferExpire = Optional.ofNullable(bufferExpire).orElse(0);
        this.bufferExcludeFields = bufferExcludeFields;
        this.script = Optional.ofNullable(script).orElse("");
    }

    /**
     * Called when new data arrives to the sink, and forwards it to Redis channel.
     *
     * @param input The incoming data
     */
    @Override
    public void invoke(T input, Context context) {
        RedisRecord<byte[], Map<String, Object>> redisRecord = redisSchema.serialize(input);
        if (redisRecord.getKind() != RowKind.INSERT && redisRecord.getKind() != RowKind.UPDATE_AFTER) {
            return;
        }
        String key = new String(redisRecord.getKey());
        if ("{}".equals(key)) {
            key = keyPrefix + ":{" + tableName + "}";
        } else {
            key = keyPrefix + ":" + tableName + ":" + key;
        }
        Map<String, Object> recordValue = redisRecord.getValue();
        Map<String, String> value = new HashMap<>();
        recordValue.forEach((k, v) ->
            value.put(k, Optional.ofNullable(v).map(Object::toString).orElse(NULL_VALUE))
        );

        if (latestRecordMap != null) {
            // 记录最大的时间戳
            if (context.timestamp() != null) {
                expireTimestamp = Math.max(expireTimestamp, context.timestamp() - bufferExpire);
            } else {
                expireTimestamp = Math.max(expireTimestamp, context.currentWatermark() - bufferExpire);
            }
            Map<String, String> recordForBuffer;
            if (CollectionUtils.isEmpty(bufferExcludeFields)) {
                recordForBuffer = value;
            } else {
                recordForBuffer = value
                    .entrySet()
                    .parallelStream()
                    .filter(x -> !bufferExcludeFields.contains(x.getKey()))
                    .collect(
                        Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue)
                    );
            }
            if (latestRecordMap.containsKey(recordForBuffer)) {
                // 在buffer中查找到相同的记录，不再做后续处理
                return;
            }
            latestRecordMap.put(recordForBuffer, context.timestamp());
        }

        LOG.info("key={}, recordValue={}", key, recordValue);
        switch (command) {
            case HMSET:
                invokeHmset(key, value);
                break;
            case EVAL:
                invokeEval(key, value);
                break;
            default:
                // do nothing
        }
    }

    private void invokeHmset(String key, Map<String, String> value) {
        RMap<String, String> rmap = redisson.getMap(key, new StringCodec());
        rmap.putAll(value);
        if (ttl > 0) {
            rmap.expire(ttl, TimeUnit.SECONDS);
        }
    }

    private void invokeEval(String key, Map<String, String> value) {
        RScript rScript = redisson.getScript(new StringCodec());
        for (;;) {
            try {
                if (ttl > 0) {
                    rScript.evalSha(
                        key,
                        RScript.Mode.READ_WRITE,
                        digest,
                        RScript.ReturnType.BOOLEAN,
                        Collections.singletonList(key),
                        JSON.toJSONString(value),
                        ttl);
                } else {
                    rScript.evalSha(
                        key,
                        RScript.Mode.READ_WRITE,
                        digest,
                        RScript.ReturnType.BOOLEAN,
                        Collections.singletonList(key),
                        JSON.toJSONString(value));
                }
                return;
            } catch (RedisException e) {
                LOG.warn(e.getMessage(), e);
                digest = rScript.scriptLoad(script);
            }
        }
    }

    /**
     * Initializes the connection to Redis by either cluster or sentinels or single server.
     */
    @Override
    public void open(Configuration parameters) {
        redisson = Redisson.create(buildRedissonConfig(serverOption));
        if (StringUtils.isNotBlank(script)) {
            digest = redisson.getScript(new StringCodec()).scriptLoad(script);
        }
        if (bufferExpire > 0) {
            latestRecordMap = new LinkedHashMap<Map<String, String>, Long>() {
                @Override
                protected boolean removeEldestEntry(Map.Entry<Map<String, String>, Long> eldest) {
                    Long timestamp = eldest.getValue();
                    return (timestamp == null || timestamp <= expireTimestamp);
                }
            };
        }
    }

    @Override
    public void close() {
        if (redisson != null) {
            redisson.shutdown();
        }
    }

    private Config buildRedissonConfig(RedisOption.ServerOption serverOption) {
        Config redissonConfig = new Config();
        if (RedisOption.SERVER_MODE_SENTINEL.equalsIgnoreCase(serverOption.getServerMode())) {
            redissonConfig.useSentinelServers()
                .setMasterName(serverOption.getMasterName())
                .setPassword(serverOption.getPassword())
                .setDatabase(serverOption.getDatabase())
                .addSentinelAddress(serverOption.getSentinels().toArray(new String[0]))
                .setReadMode(ReadMode.MASTER_SLAVE);
        } else if (RedisOption.SERVER_MODE_CLUSTER.equalsIgnoreCase(serverOption.getServerMode())) {
            redissonConfig.useClusterServers()
                .setPassword(serverOption.getPassword())
                .addNodeAddress(serverOption.getNodes().toArray(new String[0]))
                .setReadMode(ReadMode.MASTER_SLAVE);
        } else {
            // 默认为单机模式，调试用
            redissonConfig.useSingleServer()
                .setAddress(serverOption.getHost() + ":" + serverOption.getPort())
                .setPassword(serverOption.getPassword())
                .setDatabase(serverOption.getDatabase());
        }

        return redissonConfig;
    }
}
