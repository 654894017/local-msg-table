package com.damon.localmsgtx.client;


import com.damon.localmsgtx.exception.TxMsgDuplicateKeyException;
import com.damon.localmsgtx.exception.TxMsgException;

public interface ITxMsgClient {

    /**
     * send transactional message
     *
     * @param msgKey
     * @param content
     * @return
     * @throws TxMsgDuplicateKeyException if the message key already exists
     * @throws TxMsgException             if other exceptions
     */
    Long sendTxMsg(String msgKey, String content) throws TxMsgDuplicateKeyException, TxMsgException;

    /**
     * resend all unsent messages
     *
     * @param shardTailNumber
     */
    void resendWaitingTxMsg(String shardTailNumber);

    /**
     * clean expired messages
     *
     * @param expireTime Expiration timestamp (milliseconds)
     */
    void cleanExpiredTxMsg(Long expireTime);
}