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
        props.put(ProducerConfig.ACKS_CONFIG, "0");
        props.put(ProducerConfig.RETRIES_CONFIG, 3);
        return new KafkaProducer<>(props);
    }

    public static KafkaProducer<String, String> customProducer(Properties properties) {
        return new KafkaProducer<>(properties);
    }

    public static KafkaProducer<String, String> customProducer(Map<String, Object> properties) {
        return new KafkaProducer<>(properties);
    }

}
