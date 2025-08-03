package com.damon.localmsgtx.config;

import com.damon.localmsgtx.store.TxMsgSqlStore;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;

import javax.sql.DataSource;

public class TxMsgKafkaConfig {

    private String txMsgTableName;

    private DataSource dataSource;

    private String topic;

    private String feedbackTopic;

    private KafkaProducer<String, String> kafkaProducer;

    private boolean enableFeedbackMsgConsumer;

    private KafkaConsumer<String, String> kafkaConsumer;

    public String getTxMsgTableName() {
        return txMsgTableName;
    }

    public void setTxMsgTableName(String txMsgTableName) {
        this.txMsgTableName = txMsgTableName;
    }

    public DataSource getDataSource() {
        return dataSource;
    }

    public void setDataSource(DataSource dataSource) {
        this.dataSource = dataSource;
    }

    public KafkaProducer<String, String> getKafkaProducer() {
        return kafkaProducer;
    }

    public void setKafkaProducer(KafkaProducer<String, String> kafkaProducer) {
        this.kafkaProducer = kafkaProducer;
    }

    public boolean isEnableFeedbackMsgConsumer() {
        return enableFeedbackMsgConsumer;
    }

    public void setEnableFeedbackMsgConsumer(boolean enableFeedbackMsgConsumer) {
        this.enableFeedbackMsgConsumer = enableFeedbackMsgConsumer;
    }

    public KafkaConsumer<String, String> getKafkaConsumer() {
        return kafkaConsumer;
    }

    public void setKafkaConsumer(KafkaConsumer<String, String> kafkaConsumer) {
        this.kafkaConsumer = kafkaConsumer;
    }

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public String getFeedbackTopic() {
        return feedbackTopic;
    }

    public void setFeedbackTopic(String feedbackTopic) {
        this.feedbackTopic = feedbackTopic;
    }

    public TxMsgSqlStore getTxMsgSqlStore() {
        return new TxMsgSqlStore(dataSource, txMsgTableName, topic, feedbackTopic);
    }
}