package com.damon.localmsgtx.client.impl;

import com.damon.localmsgtx.client.ITxFeedbackMsgClient;
import com.damon.localmsgtx.handler.FeedbackTxMsgHandler;
import com.damon.localmsgtx.model.TxMsgFeedback;

public class KafkaTxFeedbackMsgClient implements ITxFeedbackMsgClient {

    private final FeedbackTxMsgHandler feedbackTxMsgHandler;

    public KafkaTxFeedbackMsgClient(FeedbackTxMsgHandler feedbackTxMsgHandler) {
        this.feedbackTxMsgHandler = feedbackTxMsgHandler;
    }

    @Override
    public void sendFeedbackMsg(TxMsgFeedback feedback) {
        feedbackTxMsgHandler.sendFeedbackMsg(feedback);
    }
}
