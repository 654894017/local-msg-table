package com.damon.localmsgtx.config.factory;

import com.damon.localmsgtx.exception.TxMsgException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;

public class RocketProducerFactory {
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
