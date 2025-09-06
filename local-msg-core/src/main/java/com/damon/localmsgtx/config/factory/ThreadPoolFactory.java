package com.damon.localmsgtx.config.factory;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class ThreadPoolFactory {
    /**
     * 基于jdk21的虚拟线程池
     *
     * @return
     */
    public static ExecutorService simpleThreadPool() {
        // 创建异步发送线程池,用于发送事务消息到kafka
        return Executors.newThreadPerTaskExecutor(
                r -> {
                    Thread thread = Thread.ofVirtual().unstarted(r);
                    thread.setName("kafka-tx-msg-sender-" + thread.threadId());
                    return thread;
                }
        );
    }
}
