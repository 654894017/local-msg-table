package demo;

import com.damon.localmsgtx.ITxMsgClient;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/api")
public class OrderDemoController {
    @Autowired
    private OrderService orderService;
    @Autowired
    private ITxMsgClient txMsgClient;

    /**
     * 创建订单并发送事务消息
     */
    @PostMapping("/orders")
    public String createOrder(@RequestParam String orderId,
                              @RequestParam String product,
                              @RequestParam int quantity) {
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
    public String resendFailedMessages() {
        txMsgClient.resendFailedTxMsg();
        return "Resend task triggered";
    }

    /**
     * 清理过期消息
     */
    @DeleteMapping("/cleanup")
    public String cleanupExpiredMessages() {
        // 清理1小时前的过期消息
        long oneHourAgo = System.currentTimeMillis() - 60 * 60 * 1000;
        txMsgClient.cleanExpiredTxMsg(oneHourAgo);
        return "Cleanup task triggered";
    }
}