package org.zhanglu.flink.connector.redis;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.serialization.SerializationSchema;

import java.io.Serializable;
import java.util.Map;

/**
 * A {@link RedisSerializationSchema} defines how to serialize values of type {@code T} into {@link
 * RedisRecord ProducerRecords}.
 *
 * @param <T> the type of values being serialized
 * @author zhanglu
 * @date 2021/5/4 21:14
 */
@PublicEvolving
public interface RedisSerializationSchema<T> extends Serializable {

    /**
     * Initialization method for the schema. It is called before the actual working methods {@link
     * #serialize(Object)} and thus suitable for one time setup work.
     *
     * <p>The provided {@link SerializationSchema.InitializationContext} can be used to access
     * additional features such as e.g. registering user metrics.
     *
     * @param context Contextual information that can be used during initialization.
     * @throws Exception
     */
    default void open(SerializationSchema.InitializationContext context) throws Exception {
    }

    /**
     * Serializes given element and returns it as a {@link RedisRecord}.
     *
     * @param element element to be serialized
     * @return Kafka {@link RedisRecord}
     */
    RedisRecord<byte[], Map<String, Object>> serialize(T element);
}
