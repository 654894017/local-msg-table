package com.damon.localmsgtx.feedback;

import com.damon.localmsgtx.store.TxMsgSqlStore;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.Logger;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

public class TxFeedbackMsgDispatch {

    private final Logger logger = org.slf4j.LoggerFactory.getLogger(TxFeedbackMsgDispatch.class);

    private final int concurrentThreadNumber;

    private final TxMsgSqlStore txMsgSqlStore;

    private final KafkaConsumer<String, String> kafkaConsumer;

    private final ExecutorService threadPoolExecutor;

    public TxFeedbackMsgDispatch(KafkaConsumer<String, String> kafkaConsumer, int concurrentThreadNumber, TxMsgSqlStore txMsgSqlStore) {
        this.kafkaConsumer = kafkaConsumer;
        this.concurrentThreadNumber = concurrentThreadNumber;
        this.txMsgSqlStore = txMsgSqlStore;
        // 使用自定义ThreadFactory创建线程池
        this.threadPoolExecutor = Executors.newFixedThreadPool(concurrentThreadNumber, new TxFeedbackThreadFactory());
        init();
    }

    private void init() {
        for (int i = 0; i < concurrentThreadNumber; i++) {
            threadPoolExecutor.submit(new TxFeedbackMsgHandler(txMsgSqlStore, kafkaConsumer));
        }
        logger.info("TxFeedbackMsgHandler init finished, current thread number : {}", concurrentThreadNumber);
    }

    private static class TxFeedbackThreadFactory implements ThreadFactory {
        private final ThreadGroup group;
        private final AtomicInteger threadNumber = new AtomicInteger(1);
        private final String namePrefix;

        public TxFeedbackThreadFactory() {
            group = Thread.currentThread().getThreadGroup();
            namePrefix = "tx-feedback-handler-";
        }

        @Override
        public Thread newThread(Runnable r) {
            Thread t = new Thread(group, r,
                    namePrefix + threadNumber.getAndIncrement(),
                    0);
            if (t.isDaemon()) {
                t.setDaemon(false);
            }
            if (t.getPriority() != Thread.NORM_PRIORITY) {
                t.setPriority(Thread.NORM_PRIORITY);
            }
            return t;
        }
    }
}
