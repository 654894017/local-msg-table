package com.damon.localmsgtx.client.impl;

import com.damon.localmsgtx.client.ITxFeedbackMsgClient;
import com.damon.localmsgtx.handler.TxFeedbackMsgSender;
import com.damon.localmsgtx.model.TxMsgFeedback;
import org.apache.kafka.clients.producer.KafkaProducer;

public class KafkaTxFeedbackMsgClient implements ITxFeedbackMsgClient {

    private final TxFeedbackMsgSender txFeedbackMsgSender;

    public KafkaTxFeedbackMsgClient(KafkaProducer<String, String> feedbackProducer, String feedbackTopic) {
        this.txFeedbackMsgSender = new TxFeedbackMsgSender(feedbackProducer, feedbackTopic);
    }

    @Override
    public void sendFeedbackMsg(TxMsgFeedback feedback) {
        txFeedbackMsgSender.sendFeedbackMsg(feedback);
    }
}
