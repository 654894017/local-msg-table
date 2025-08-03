package com.damon.localmsgtx.feedback;


import com.damon.localmsgtx.store.TxMsgSqlStore;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.Logger;

public class TxFeedbackMsgHandler {

    private final Logger logger = org.slf4j.LoggerFactory.getLogger(TxFeedbackMsgHandler.class);

    private final int concurrentThreadNumber;

    private final TxMsgSqlStore txMsgSqlStore;

    private final KafkaConsumer<String, String> kafkaConsumer;

    public TxFeedbackMsgHandler(KafkaConsumer<String, String> kafkaConsumer, int concurrentThreadNumber, TxMsgSqlStore txMsgSqlStore) {
        this.kafkaConsumer = kafkaConsumer;
        this.concurrentThreadNumber = concurrentThreadNumber;
        this.txMsgSqlStore = txMsgSqlStore;
        init();
    }

    private void init() {
        for (int i = 0; i < concurrentThreadNumber; i++) {
            new Thread(new TxFeedbackMsgDiapatch(txMsgSqlStore, kafkaConsumer)).start();
        }
        logger.info("TxFeedbackMsgHandler init finished, current thread number : {}", concurrentThreadNumber);

    }


}
