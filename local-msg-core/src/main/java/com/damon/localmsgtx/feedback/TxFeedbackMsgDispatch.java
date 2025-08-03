package com.damon.localmsgtx.feedback;


import com.damon.localmsgtx.store.TxMsgSqlStore;
import org.apache.kafka.clients.consumer.KafkaConsumer;

public class TxFeedbackMsgDispatch {

    private final int concurrentThreadNumber;

    private final TxMsgSqlStore txMsgSqlStore;

    private final KafkaConsumer<String, String> kafkaConsumer;

    public TxFeedbackMsgDispatch(KafkaConsumer<String, String> kafkaConsumer, int concurrentThreadNumber, TxMsgSqlStore txMsgSqlStore) {
        this.kafkaConsumer = kafkaConsumer;
        this.concurrentThreadNumber = concurrentThreadNumber;
        this.txMsgSqlStore = txMsgSqlStore;
        init();
    }

    private void init() {
        for (int i = 0; i < concurrentThreadNumber; i++) {
            new Thread(new TxFeedbackMsgConsumer(txMsgSqlStore, kafkaConsumer)).start();
        }
    }


}
