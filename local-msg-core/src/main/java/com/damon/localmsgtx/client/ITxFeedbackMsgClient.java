package com.damon.localmsgtx.client;


import com.damon.localmsgtx.model.TxMsgFeedback;

public interface ITxFeedbackMsgClient {
    void sendFeedbackMsg(TxMsgFeedback feedback);

}