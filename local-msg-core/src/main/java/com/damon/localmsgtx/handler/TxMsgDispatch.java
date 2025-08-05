package com.damon.localmsgtx.handler;

import com.damon.localmsgtx.utils.DefaultThreadFactory;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.Logger;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class TxMsgDispatch<T> {

    private final Logger logger = org.slf4j.LoggerFactory.getLogger(TxMsgDispatch.class);

    private final int concurrentThreadNumber;

    private final KafkaConsumer<String, String> kafkaConsumer;

    private final ExecutorService threadPoolExecutor;

    public TxMsgDispatch(KafkaConsumer<String, String> kafkaConsumer, int concurrentThreadNumber) {
        this.kafkaConsumer = kafkaConsumer;
        this.concurrentThreadNumber = concurrentThreadNumber;
        // 使用自定义ThreadFactory创建线程池
        this.threadPoolExecutor = Executors.newFixedThreadPool(
                concurrentThreadNumber,
                new DefaultThreadFactory("tx-feedback-handler-")
        );
        init();
    }

    private void init() {
        for (int i = 0; i < concurrentThreadNumber; i++) {
            threadPoolExecutor.submit(new TxMsgKafkaHandler<T>(kafkaConsumer));
        }
        logger.info("TxFeedbackMsgHandler init finished, current thread number : {}", concurrentThreadNumber);
    }

}
