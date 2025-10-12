package com.damon.localmsgtx.config.factory;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Map;
import java.util.Properties;

public class KafkaProducerFactory {
    public static KafkaProducer<String, String> simpleProducer(String server) {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, server);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.ACKS_CONFIG, "1");
        props.put(ProducerConfig.RETRIES_CONFIG, 3);
        // 批量发送相关配置
        // 批次大小，单位字节，默认16384(16KB)
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, 65536);
        // 超过收集的时间的最大等待时长，单位：毫秒。
        props.put(ProducerConfig.LINGER_MS_CONFIG, 10);
        // 启用压缩，可选gzip、snappy、lz4
        props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy");
        // 缓冲区大小，默认33554432(32MB)
        props.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 67108864);
        return new KafkaProducer<>(props);
    }

    public static KafkaProducer<String, String> customProducer(Properties properties) {
        return new KafkaProducer<>(properties);
    }

    public static KafkaProducer<String, String> customProducer(Map<String, Object> properties) {
        return new KafkaProducer<>(properties);
    }

}
