package com.damon.localmsgtx;

import org.apache.kafka.clients.producer.KafkaProducer;

import javax.sql.DataSource;

public class TxMsgKafkaConfig {

    private String txMsgTableName;

    private DataSource dataSource;

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
}