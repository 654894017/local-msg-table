package com.damon.localmsgtx.config.factory;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * 事务消息发送线程池工厂
 * <p>
 * 基于JDK 21虚拟线程，创建轻量级线程池用于异步消息发送。
 */
public class TxMsgSenderThreadPoolFactory {

    /**
     * 创建基于虚拟线程的线程池
     *
     * @return 虚拟线程执行器
     */
    public static ExecutorService simpleThreadPool() {
        return Executors.newThreadPerTaskExecutor(
                r -> {
                    Thread thread = Thread.ofVirtual().unstarted(r);
                    thread.setName("tx-msg-sender-" + thread.threadId());
                    return thread;
                }
        );
    }
}
