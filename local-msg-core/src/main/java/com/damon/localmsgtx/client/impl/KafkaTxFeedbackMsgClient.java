package com.damon.localmsgtx.client.impl;

import com.damon.localmsgtx.client.ITxFeedbackMsgClient;
import com.damon.localmsgtx.handler.TxFeedbackMsgSender;
import com.damon.localmsgtx.model.TxMsgFeedback;

public class KafkaTxFeedbackMsgClient implements ITxFeedbackMsgClient {

    private final TxFeedbackMsgSender txFeedbackMsgSender;

    public KafkaTxFeedbackMsgClient(TxFeedbackMsgSender txFeedbackMsgSender) {
        this.txFeedbackMsgSender = txFeedbackMsgSender;
    }

    @Override
    public void sendFeedbackMsg(TxMsgFeedback feedback) {
        txFeedbackMsgSender.sendFeedbackMsg(feedback);
    }
}
