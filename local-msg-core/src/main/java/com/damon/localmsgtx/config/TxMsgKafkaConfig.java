package com.damon.localmsgtx.config;

import com.damon.localmsgtx.store.TxMsgSqlStore;
import org.apache.kafka.clients.producer.KafkaProducer;

import javax.sql.DataSource;

public class TxMsgKafkaConfig {

    private String txMsgTableName;

    private DataSource dataSource;

    private String topic;

    private KafkaProducer<String, String> kafkaProducer;

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

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public TxMsgSqlStore getTxMsgSqlStore() {
        return new TxMsgSqlStore(dataSource, txMsgTableName, topic);
    }
}