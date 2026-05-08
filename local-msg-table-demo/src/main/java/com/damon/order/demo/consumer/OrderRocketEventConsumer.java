package com.damon.order.demo.consumer;

import com.damon.order.demo.config.OrderRocketTxMsgConfig;
import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.common.message.MessageExt;
import org.springframework.stereotype.Component;

import java.util.List;

/**
 * RocketMQ订单事件消费者（演示用）
 * <p>
 * 在后台线程中监听order_events主题，消费订单事务消息。
 */
@Component
public class OrderRocketEventConsumer {

    public OrderRocketEventConsumer() {
        new Thread(() -> {
            try {
                consumeMessages();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }).start();
    }

    private void consumeMessages() throws Exception {
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("order_consumer_group");
        consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_LAST_OFFSET);
        consumer.setNamesrvAddr(OrderRocketTxMsgConfig.ROCKETMQ_SERVER);
        consumer.subscribe(OrderRocketTxMsgConfig.ORDER_TOPIC, "test");

        consumer.registerMessageListener(new MessageListenerConcurrently() {
            @Override
            public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> msgs, ConsumeConcurrentlyContext context) {
                for (MessageExt msg : msgs) {
                    String content = new String(msg.getBody());
                    System.out.printf("RocketMQ received - Topic: %s, Value: %s%n", msg.getTopic(), content);
                }
                return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
            }
        });

        consumer.start();
        System.out.println("RocketMQ consumer started, listening to 'order_events' topic");
    }
}
