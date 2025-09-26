package com.damon.localmsgtx.config.factory;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class TxMsgSenderThreadPoolFactory {
    /**
     * base on jdk21 virtual thread pool
     *
     * @return
     */
    public static ExecutorService simpleThreadPool() {
        // create async send thread pool, used to send transaction messages to kafka
        return Executors.newThreadPerTaskExecutor(
                r -> {
                    Thread thread = Thread.ofVirtual().unstarted(r);
                    thread.setName("kafka-tx-msg-sender-" + thread.threadId());
                    return thread;
                }
        );
    }
}
