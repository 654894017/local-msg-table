package com.damon.order.demo;

import com.damon.localmsgtx.client.ITxMsgClient;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

/**
 * 订单服务
 * <p>
 * 演示事务消息的完整使用流程：创建订单、补偿重发、过期清理。
 */
@Slf4j
@Service
public class OrderService {

    @Autowired
    private JdbcTemplate jdbcTemplate;

    @Autowired
    @Qualifier("kafkaTxMsgClient")
    private ITxMsgClient kafkaTxMsgClient;

    @Autowired
    @Qualifier("rocketTxMsgClient")
    private ITxMsgClient rocketTxMsgClient;

    /**
     * 创建订单并发送事务消息
     * <p>
     * 订单创建和消息发送在同一本地事务中，确保一致性。
     * 消息将在事务提交后异步发送到MQ。
     */
    @Transactional(rollbackFor = Exception.class)
    public void createOrder(String orderId, String product, int quantity) {
        // 创建订单记录
        String insertOrderSql = "INSERT INTO orders (order_id, product, quantity, status) VALUES (?, ?, ?, ?)";
        jdbcTemplate.update(insertOrderSql, orderId, product, quantity, "CREATED");

        // 发送事务消息（事务提交后投递）
        String messageContent = String.format("{\"orderId\":\"%s\",\"product\":\"%s\",\"quantity\":%d}",
                orderId, product, quantity);

        Long rocketmqMsgId = rocketTxMsgClient.sendTxMsg(orderId, "test", messageContent);
        log.info("订单创建完成，RocketMQ事务消息已注册, msgId: {}", rocketmqMsgId);

        Long kafkaMsgId = kafkaTxMsgClient.sendTxMsg(orderId, messageContent);
        log.info("订单创建完成，Kafka事务消息已注册, msgId: {}", kafkaMsgId);
    }

    /**
     * 手动触发补偿重发
     */
    public void resendWaitingTxMsg() {
        kafkaTxMsgClient.resendWaitingTxMsg();
        rocketTxMsgClient.resendWaitingTxMsg();
    }

    /**
     * 清理过期消息（清理1小时前已发送的消息记录）
     */
    public void cleanupExpiredMessages() {
        long oneHourAgo = System.currentTimeMillis() - 60 * 60 * 1000;
        kafkaTxMsgClient.cleanExpiredTxMsg(oneHourAgo);
        rocketTxMsgClient.cleanExpiredTxMsg(oneHourAgo);
    }
}
