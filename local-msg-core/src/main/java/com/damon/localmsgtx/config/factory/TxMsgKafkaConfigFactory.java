package com.damon.localmsgtx.config.factory;

import com.damon.localmsgtx.config.TxMsgKafkaConfig;
import org.apache.kafka.clients.producer.KafkaProducer;

import javax.sql.DataSource;
import java.util.concurrent.ExecutorService;

public class TxMsgKafkaConfigFactory {

    public static TxMsgKafkaConfig simpleConfig(String kafkaServer, String topic, DataSource dataSource, String txMsgTableName) {

        ExecutorService asyncSendExecutor = ThreadPoolFactory.simpleThreadPool();

        KafkaProducer<String, String> producer = KafkaProducerFactory.simpleProducer(kafkaServer);

        TxMsgKafkaConfig config = new TxMsgKafkaConfig();
        config.setDataSource(dataSource);
        config.setTopic(topic);
        config.setKafkaProducer(producer);
        config.setTxMsgTableName(txMsgTableName);
        config.setAsyncSendExecutor(asyncSendExecutor);
        return config;
    }
}