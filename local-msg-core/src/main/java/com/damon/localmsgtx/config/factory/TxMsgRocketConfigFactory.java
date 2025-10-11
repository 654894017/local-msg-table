package com.damon.localmsgtx.config.factory;

import com.damon.localmsgtx.config.TxMsgConfig;
import com.damon.localmsgtx.handler.AbstractTxMsgHandler;
import com.damon.localmsgtx.handler.RocketTxMsgHandler;
import com.damon.localmsgtx.store.TxMsgSqlStore;
import org.apache.rocketmq.client.producer.DefaultMQProducer;

import javax.sql.DataSource;
import java.util.concurrent.ExecutorService;

public class TxMsgRocketConfigFactory {

    private final static int RANMON_FACTOR_LENGTH = 6;

    public static TxMsgConfig simpleConfig(String namesrvAddr, String topic, String producerGroup, DataSource dataSource, String txMsgTableName) {

        ExecutorService asyncSendExecutor = TxMsgSenderThreadPoolFactory.simpleThreadPool();

        DefaultMQProducer producer = RocketProducerFactory.simpleProducer(namesrvAddr, producerGroup);

        TxMsgSqlStore txMsgSqlStore = new TxMsgSqlStore(dataSource, txMsgTableName, topic, RANMON_FACTOR_LENGTH);

        AbstractTxMsgHandler txMsgHandler = new RocketTxMsgHandler(producer, txMsgSqlStore);

        return new TxMsgConfig(asyncSendExecutor, txMsgHandler);

    }
}