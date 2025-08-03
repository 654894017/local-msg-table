package com.damon.localmsgtx.config;

import org.apache.kafka.clients.producer.KafkaProducer;

public class TxMsgFeedbackKafkaConfig {

    private String topic;

    private KafkaProducer<String, String> kafkaProducer;

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public KafkaProducer<String, String> getKafkaProducer() {
        return kafkaProducer;
    }

    public void setKafkaProducer(KafkaProducer<String, String> kafkaProducer) {
        this.kafkaProducer = kafkaProducer;
    }
}