package com.damon.localmsgtx.config.factory;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Map;
import java.util.Properties;

/**
 * Kafka生产者工厂
 * <p>
 * 提供简单配置和自定义配置两种方式创建KafkaProducer。
 */
public class KafkaProducerFactory {

    /**
     * 创建简单配置的Kafka生产者
     * <p>
     * 默认配置：acks=1, retries=3, batchSize=64KB, lingerMs=10ms, snappy压缩, 缓冲区64MB
     *
     * @param server Kafka服务地址（broker列表）
     * @return Kafka生产者实例
     */
    public static KafkaProducer<String, String> simpleProducer(String server) {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, server);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.ACKS_CONFIG, "1");
        props.put(ProducerConfig.RETRIES_CONFIG, 3);
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, 65536);           // 批次大小 64KB
        props.put(ProducerConfig.LINGER_MS_CONFIG, 10);               // 最大等待时间 10ms
        props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy");  // snappy压缩
        props.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 67108864);     // 缓冲区 64MB
        return new KafkaProducer<>(props);
    }

    /**
     * 使用自定义Properties创建Kafka生产者
     *
     * @param properties 自定义配置
     * @return Kafka生产者实例
     */
    public static KafkaProducer<String, String> customProducer(Properties properties) {
        return new KafkaProducer<>(properties);
    }

    /**
     * 使用自定义Map配置创建Kafka生产者
     *
     * @param properties 自定义配置Map
     * @return Kafka生产者实例
     */
    public static KafkaProducer<String, String> customProducer(Map<String, Object> properties) {
        return new KafkaProducer<>(properties);
    }
}
