package com.damon.order.demo;

import com.damon.localmsgtx.utils.ShardTailNumber;
import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.*;

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
     * 手动触发重发失败的消息
     */
    @PostMapping("/resend")
    public String resendWaitingTxMsg() {
        ShardTailNumber shardTailNumber = new ShardTailNumber(1, 0, 1);
        shardTailNumber.generateTailNumbers().forEach(
                orderService::resendWaitingTxMsg
        );
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