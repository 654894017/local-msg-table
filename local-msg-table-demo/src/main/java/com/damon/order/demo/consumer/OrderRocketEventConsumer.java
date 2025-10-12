package com.damon.order.demo.consumer;

import com.damon.order.demo.config.OrderRocketTxMsgCongfig;
import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.common.message.MessageExt;
import org.springframework.stereotype.Component;

import java.util.List;

@Component
public class OrderRocketEventConsumer {

    public OrderRocketEventConsumer() {
        // 在后台线程中启动消费者
        new Thread(() -> {
            try {
                consumeMessages();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }).start();
    }


    private void consumeMessages() throws Exception {
        // 1. 创建消费者，指定消费者组名（同一业务用同一组名）
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("order_consumer_group");
        consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_LAST_OFFSET);
        // 2. 设置 NameServer 地址（集群环境用逗号分隔）
        consumer.setNamesrvAddr(OrderRocketTxMsgCongfig.ROCKETMQ_SERVER);
        // 3. 订阅 Topic 和 Tag（* 表示所有 Tag，也可指定具体 Tag 如 "create,paid"）
        consumer.subscribe(OrderRocketTxMsgCongfig.ORDER_TOPIC, "test");
        // 4. 注册消息监听器（并发消费）
        consumer.registerMessageListener(new MessageListenerConcurrently() {
            @Override
            public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> msgs, ConsumeConcurrentlyContext context) {
                for (MessageExt msg : msgs) {
                    String content = new String(msg.getBody());
                    System.out.printf("Rocket received message - Topic: %s,  Value: %s%n", msg.getTopic(), content);
                }
                // 8. 所有消息处理成功，返回 CONSUME_SUCCESS
                return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
            }
        });
        // 5. 启动消费者（启动后会自动拉取消息）
        consumer.start();
        System.out.println("消费者启动成功，等待接收消息...");
    }

}