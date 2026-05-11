package com.damon.localmsgtx.config.factory;

import com.damon.localmsgtx.config.TxMsgConfig;
import com.damon.localmsgtx.handler.AbstractTxMsgHandler;
import com.damon.localmsgtx.handler.RocketTxMsgHandler;
import com.damon.localmsgtx.store.TxMsgSqlStore;
import org.apache.rocketmq.client.producer.DefaultMQProducer;

import javax.sql.DataSource;
import java.util.concurrent.ExecutorService;

/**
 * RocketMQ事务消息配置工厂
 * <p>
 * 提供快速创建RocketMQ事务消息客户端配置的便捷方法。
 */
public class TxMsgRocketConfigFactory {

    /**
     * 创建简单配置（使用默认参数）
     *
     * @param namesrvAddr    NameServer地址
     * @param topic          消息主题
     * @param producerGroup  生产者组名
     * @param dataSource     数据源
     * @param txMsgTableName 事务消息表名
     * @return 事务消息配置
     */
    public static TxMsgConfig simpleConfig(String namesrvAddr, String topic, String producerGroup,
                                           DataSource dataSource, String txMsgTableName) {
        ExecutorService asyncSendExecutor = TxMsgSenderThreadPoolFactory.simpleThreadPool();
        DefaultMQProducer producer = RocketProducerFactory.simpleProducer(namesrvAddr, producerGroup);
        TxMsgSqlStore txMsgSqlStore = new TxMsgSqlStore(dataSource, txMsgTableName, topic);
        AbstractTxMsgHandler txMsgHandler = new RocketTxMsgHandler(producer, txMsgSqlStore);
        return new TxMsgConfig(asyncSendExecutor, txMsgHandler);
    }
}
