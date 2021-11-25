package org.zhanglu.flink.connector.redis;

import org.apache.flink.types.RowKind;

/**
 * Redis记录
 *
 * @author zhanglu
 * @date 2021/4/17 00:33
 */
public class RedisRecord<K, V> {
    private final RowKind kind;
    private final K key;
    private final V value;

    public RedisRecord(RowKind kind, K key, V value) {
        this.kind = kind;
        this.key = key;
        this.value = value;
    }

    public RowKind getKind() {
        return kind;
    }

    public K getKey() {
        return key;
    }

    public V getValue() {
        return value;
    }

    @Override
    public String toString() {
        return "RedisRecord{"
                + "kind=" + kind
                + ", key=" + key
                + ", value=" + value
                + '}';
    }
}
