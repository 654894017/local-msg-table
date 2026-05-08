package com.damon.localmsgtx.config.factory;

import com.damon.localmsgtx.exception.TxMsgException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;

/**
 * RocketMQ生产者工厂
 * <p>
 * 创建并启动RocketMQ生产者实例。
 */
public class RocketProducerFactory {

    /**
     * 创建并启动简单配置的RocketMQ生产者
     *
     * @param namesrvAddr   NameServer地址
     * @param producerGroup 生产者组名
     * @return 已启动的RocketMQ生产者实例
     * @throws TxMsgException 启动失败时抛出
     */
    public static DefaultMQProducer simpleProducer(String namesrvAddr, String producerGroup) {
        DefaultMQProducer producer = new DefaultMQProducer(producerGroup);
        producer.setNamesrvAddr(namesrvAddr);
        try {
            producer.start();
        } catch (MQClientException e) {
            throw new TxMsgException(e);
        }
        return producer;
    }
}
