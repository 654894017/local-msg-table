package com.damon.localmsgtx.config;

import com.damon.localmsgtx.store.TxMsgSqlStore;
import org.apache.kafka.clients.producer.KafkaProducer;

import javax.sql.DataSource;
import java.util.concurrent.ExecutorService;

public class TxMsgKafkaConfig {

    private String txMsgTableName;

    private DataSource dataSource;

    private String topic;

    private boolean asyncSendMsg;

    private ExecutorService asyncSendExecutor;

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

    public boolean isAsyncSendMsg() {
        return asyncSendMsg;
    }

    public TxMsgSqlStore getTxMsgSqlStore() {
        return new TxMsgSqlStore(dataSource, txMsgTableName, topic);
    }

    public ExecutorService getAsyncSendExecutor() {
        return asyncSendExecutor;
    }

    public void setAsyncSendExecutor(ExecutorService asyncSendExecutor) {
        this.asyncSendExecutor = asyncSendExecutor;
        this.asyncSendMsg = true;
    }
}