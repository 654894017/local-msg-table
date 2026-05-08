package com.damon.order.demo;

import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.*;

/**
 * 订单API控制器
 * <p>
 * 提供订单创建、消息补偿重发和过期清理的HTTP接口。
 */
@RestController
@RequestMapping("/api")
@RequiredArgsConstructor
public class OrderController {

    private final OrderService orderService;

    /**
     * 创建订单并发送事务消息
     */
    @PostMapping("/orders")
    public String createOrder(@RequestParam("orderId") String orderId,
                              @RequestParam("product") String product,
                              @RequestParam("quantity") int quantity) {
        try {
            orderService.createOrder(orderId, product, quantity);
            return "Order created successfully with transactional message sent";
        } catch (Exception e) {
            return "Failed to create order: " + e.getMessage();
        }
    }

    /**
     * 手动触发补偿重发
     */
    @PostMapping("/resend")
    public String resendWaitingTxMsg() {
        orderService.resendWaitingTxMsg();
        return "Resend task triggered";
    }

    /**
     * 清理过期消息
     */
    @DeleteMapping("/cleanup")
    public String cleanupExpiredMessages() {
        orderService.cleanupExpiredMessages();
        return "Cleanup task triggered";
    }
}
