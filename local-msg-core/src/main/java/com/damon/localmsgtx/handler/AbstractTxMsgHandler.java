package com.damon.localmsgtx.handler;

import com.damon.localmsgtx.model.TxMsgModel;
import com.damon.localmsgtx.model.TxMsgStatusEnum;
import com.damon.localmsgtx.store.TxMsgSqlStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.Assert;

public abstract class AbstractTxMsgHandler {
    private static final Logger logger = LoggerFactory.getLogger(AbstractTxMsgHandler.class);
    /**
     * Batch size for deleting expired messages in a single operation
     */
    protected final int deleteBatchSize;

    protected final TxMsgSqlStore txMsgSqlStore;

    protected AbstractTxMsgHandler(int deleteBatchSize, TxMsgSqlStore txMsgSqlStore) {
        Assert.isTrue(deleteBatchSize > 0, "Delete batch size must be greater than 0");
        Assert.notNull(txMsgSqlStore, "TxMsgSqlStore cannot be null");
        this.deleteBatchSize = deleteBatchSize;
        this.txMsgSqlStore = txMsgSqlStore;
    }

    public TxMsgModel saveMsg(String content, String msgKey) {
        return txMsgSqlStore.insertTxMsg(content, msgKey);
    }

    /**
     * Delete expired sent messages
     *
     * @param expireTime Expiration timestamp (milliseconds), sent messages with timestamps less than or equal to this time will be deleted
     */
    public void deleteExpiredSentMessages(Long expireTime, TxMsgStatusEnum statusEnum) {
        Assert.notNull(expireTime, "Expiration timestamp cannot be null");
        logger.info("Starting to clean up expired sent messages, expiration time: {}ms", expireTime);
        txMsgSqlStore.deleteExpiredSendedMsg(expireTime, deleteBatchSize, statusEnum);
        logger.info("Cleanup of expired sent messages completed");
    }

    public abstract void sendMsg(TxMsgModel txMsgModel);

    public abstract void resendWaitingMessages(String shardTailNumber);

}
