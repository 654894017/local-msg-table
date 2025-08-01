package com.damon.localmsgtx;


public interface ITxMsgClient {
    Long sendTxMsg(String topic, String msgKey, String content);

    void resendFailedTxMsg();

    void cleanExpiredTxMsg(Long expireTime);
}