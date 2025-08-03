package com.damon.localmsgtx.client;


public interface ITxMsgClient {
    Long sendTxMsg(String msgKey, String content);

    void resendFailedTxMsg();

    void cleanExpiredTxMsg(Long expireTime);
}