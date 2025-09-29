package com.damon.localmsgtx.client;


import com.damon.localmsgtx.exception.TxMsgDuplicateKeyException;
import com.damon.localmsgtx.exception.TxMsgStoreException;

public interface ITxMsgClient {

    /**
     * send transactional message
     *
     * @param msgKey  Message key (must not be null or empty)
     * @param content Message content (must not be null or empty)
     * @return Message ID
     * @throws TxMsgDuplicateKeyException if the message key already exists
     * @throws TxMsgStoreException        if other exceptions occur during message processing
     * @throws IllegalArgumentException   if the message key or content is null or empty
     */
    Long sendTxMsg(String msgKey, String content) throws IllegalArgumentException, TxMsgDuplicateKeyException, TxMsgStoreException;

    /**
     * resend all unsent messages
     *
     * @param shardTailNumber
     */
    void resendWaitingTxMsg(String shardTailNumber) throws TxMsgStoreException;

    /**
     * clean expired messages
     *
     * @param expireTime Expiration timestamp (milliseconds)
     */
    void cleanExpiredTxMsg(Long expireTime) throws TxMsgStoreException;
}