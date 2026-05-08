package com.damon.localmsgtx.config.factory;

import com.damon.localmsgtx.config.TxMsgConfig;
import com.damon.localmsgtx.handler.AbstractTxMsgHandler;
import com.damon.localmsgtx.handler.KafkaTxMsgHandler;
import com.damon.localmsgtx.store.TxMsgSqlStore;
import org.apache.kafka.clients.producer.KafkaProducer;

import javax.sql.DataSource;
import java.util.concurrent.ExecutorService;

/**
 * Kafka事务消息配置工厂
 * <p>
 * 提供快速创建Kafka事务消息客户端配置的便捷方法。
 */
public class TxMsgKafkaConfigFactory {

    /**
     * 随机因子位数（用于分片路由）
     */
    private static final int RANDOM_FACTOR_LENGTH = 6;

    /**
     * 创建简单配置（使用默认参数）
     *
     * @param kafkaServer    Kafka服务地址
     * @param topic          消息主题
     * @param dataSource     数据源
     * @param txMsgTableName 事务消息表名
     * @return 事务消息配置
     */
    public static TxMsgConfig simpleConfig(String kafkaServer, String topic, DataSource dataSource, String txMsgTableName) {
        ExecutorService asyncSendExecutor = TxMsgSenderThreadPoolFactory.simpleThreadPool();
        KafkaProducer<String, String> producer = KafkaProducerFactory.simpleProducer(kafkaServer);
        TxMsgSqlStore txMsgSqlStore = new TxMsgSqlStore(dataSource, txMsgTableName, topic, RANDOM_FACTOR_LENGTH);
        AbstractTxMsgHandler txMsgHandler = new KafkaTxMsgHandler(producer, txMsgSqlStore);
        return new TxMsgConfig(asyncSendExecutor, txMsgHandler);
    }
}
