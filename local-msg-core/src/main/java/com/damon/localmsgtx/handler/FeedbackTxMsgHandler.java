package com.damon.localmsgtx.handler;

import com.alibaba.fastjson2.JSONObject;
import com.damon.localmsgtx.TxMsgException;
import com.damon.localmsgtx.model.TxMsgFeedback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;


public class FeedbackTxMsgHandler {

    private final KafkaProducer<String, String> kafkaProducer;

    private final String feedbackTopic;

    public FeedbackTxMsgHandler(KafkaProducer<String, String> kafkaProducer, String feedbackTopic) {
        this.kafkaProducer = kafkaProducer;
        this.feedbackTopic = feedbackTopic;
    }

    public void sendFeedbackMsg(TxMsgFeedback msg) {
        ProducerRecord<String, String> record = new ProducerRecord<>(
                feedbackTopic,
                msg.getMsgKey(),
                JSONObject.toJSONString(msg)
        );
        try {
            kafkaProducer.send(record).get();
        } catch (Exception e) {
            throw new TxMsgException(e);
        }
    }
}