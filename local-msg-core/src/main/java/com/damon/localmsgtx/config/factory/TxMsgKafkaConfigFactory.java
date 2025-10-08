package com.damon.localmsgtx.config.factory;

import com.damon.localmsgtx.config.TxMsgConfig;
import com.damon.localmsgtx.handler.AbstractTxMsgHandler;
import com.damon.localmsgtx.handler.KafkaTxMsgHandler;
import com.damon.localmsgtx.store.TxMsgSqlStore;
import org.apache.kafka.clients.producer.KafkaProducer;

import javax.sql.DataSource;
import java.util.concurrent.ExecutorService;

public class TxMsgKafkaConfigFactory {

    public static TxMsgConfig simpleConfig(String kafkaServer, String topic, DataSource dataSource, String txMsgTableName) {

        ExecutorService asyncSendExecutor = TxMsgSenderThreadPoolFactory.simpleThreadPool();
        KafkaProducer<String, String> producer = KafkaProducerFactory.simpleProducer(kafkaServer);
        TxMsgSqlStore txMsgSqlStore = new TxMsgSqlStore(dataSource, txMsgTableName, topic, 6);
        AbstractTxMsgHandler txMsgHandler = new KafkaTxMsgHandler(producer, txMsgSqlStore);

        TxMsgConfig config = new TxMsgConfig();
        config.setTxMsgHandler(txMsgHandler);
        config.setAsyncSendExecutor(asyncSendExecutor);
        return config;
    }
}