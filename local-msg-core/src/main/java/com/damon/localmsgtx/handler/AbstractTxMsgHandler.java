package com.damon.localmsgtx.handler;

import com.damon.localmsgtx.model.TxMsgModel;
import com.damon.localmsgtx.model.TxMsgStatusEnum;
import com.damon.localmsgtx.store.TxMsgSqlStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.Assert;

import java.util.List;

public abstract class AbstractTxMsgHandler {
    private static final Logger logger = LoggerFactory.getLogger(AbstractTxMsgHandler.class);
    /**
     * Batch size for deleting expired messages in a single operation
     */
    protected final int deleteBatchSize;

    protected final TxMsgSqlStore txMsgSqlStore;
    /**
     * Maximum number of pending messages to fetch in a single request
     */
    protected final int fetchLimit;
    /**
     * Maximum number of messages to process in a single resend task (to prevent tasks from being too long)
     */
    protected final int maxResendNumPerTask;

    protected AbstractTxMsgHandler(int deleteBatchSize, TxMsgSqlStore txMsgSqlStore, int fetchLimit, int maxResendNumPerTask) {
        Assert.isTrue(deleteBatchSize > 0, "Delete batch size must be greater than 0");
        Assert.notNull(txMsgSqlStore, "TxMsgSqlStore cannot be null");
        Assert.isTrue(fetchLimit > 0, "Fetch limit must be greater than 0");
        Assert.isTrue(maxResendNumPerTask > 0, "Maximum resend number per task must be greater than 0");
        Assert.isTrue(deleteBatchSize > 0, "Delete batch size must be greater than 0");
        this.deleteBatchSize = deleteBatchSize;
        this.txMsgSqlStore = txMsgSqlStore;
        this.fetchLimit = fetchLimit;
        this.maxResendNumPerTask = maxResendNumPerTask;
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

    /**
     * Send a single transactional message to Kafka
     *
     * @param txMsgModel Transactional message model (cannot be null)
     */
    public void sendMsg(TxMsgModel txMsgModel) {
        Assert.notNull(txMsgModel, "Transactional message model cannot be null");
        Assert.hasText(txMsgModel.getTopic(), "Message topic cannot be empty");
        Assert.hasText(txMsgModel.getContent(), "Message content cannot be empty");
        sendMessage(txMsgModel);
    }


    /**
     * Resend all messages in "waiting to send" status
     * Used for compensation mechanism to ensure unsent messages are retried
     */
    public void resendWaitingMessages(String shardTailNumber) {
        int totalProcessed = 0;
        int currentFetchNum;
        Long maxId = 0L;

        // Loop to fetch and process messages until no more messages or reaching the single task processing limit
        do {
            // Avoid exceeding the maximum processing limit
            if (totalProcessed >= maxResendNumPerTask) {
                logger.warn("Single resend task has reached the maximum processing limit: {}, remaining messages will be processed in the next task", maxResendNumPerTask);
                break;
            }

            // Fetch pending messages
            List<TxMsgModel> waitingMessages = txMsgSqlStore.getWaitingMessages(fetchLimit, maxId, shardTailNumber);
            currentFetchNum = waitingMessages.size();
            totalProcessed += currentFetchNum;

            if (currentFetchNum == 0) {
                logger.debug("No messages pending resend");
                break;
            }

            logger.info("Starting to process batch messages, count: {}, total processed: {}, shardTailNumber: {}", currentFetchNum, totalProcessed, shardTailNumber);

            // Batch send messages
            doBatchSendMessages(waitingMessages, shardTailNumber);

            maxId = waitingMessages.get(currentFetchNum - 1).getId();

        } while (currentFetchNum == fetchLimit); // If this fetch is full, there may be more messages
        
        logger.info("Resend task completed, total messages processed in this run: {}", totalProcessed);
    }
    
    private void doBatchSendMessages(List<TxMsgModel> txMsgModels, String shardTailNumber) {
        try {
            batchSendMessages(txMsgModels);
        } catch (Exception e) {
            logger.error("Error while processing batch messages, shardTailNumber: {}", shardTailNumber, e);
        }
    }

    protected abstract void sendMessage(TxMsgModel txMsgModel);

    protected abstract void batchSendMessages(List<TxMsgModel> txMsgModels);

}
