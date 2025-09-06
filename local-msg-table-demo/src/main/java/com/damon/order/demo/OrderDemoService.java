package com.damon.order.demo;

import com.damon.localmsgtx.client.ITxMsgClient;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Slf4j
@Service
@RequiredArgsConstructor
public class OrderDemoService {
    private final JdbcTemplate jdbcTemplate;
    private final ITxMsgClient txMsgClient;

    /**
     * 创建订单并发送事务消息
     * 这里演示了事务一致性：订单创建和消息发送在同一事务中
     */
    @Transactional(rollbackFor = Exception.class)
    public void createOrder(String orderId, String product, int quantity) {

        // 1. 创建订单记录
        String insertOrderSql = "INSERT INTO orders (order_id, product, quantity, status) VALUES (?, ?, ?, ?)";
        jdbcTemplate.update(insertOrderSql, orderId, product, quantity, "CREATED");

        // 2. 发送事务消息（将在事务提交后发送）
        String messageContent = String.format("{\"orderId\":\"%s\",\"product\":\"%s\",\"quantity\":%d}",
                orderId, product, quantity);

        Long msgId = txMsgClient.sendTxMsg(orderId, messageContent);

        log.info("Order created and transactional message registered, msgId: " + msgId);
    }
}