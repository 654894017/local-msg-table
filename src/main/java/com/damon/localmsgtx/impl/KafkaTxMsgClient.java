package com.damon.localmsgtx.impl;

import com.damon.localmsgtx.ITxMsgClient;
import com.damon.localmsgtx.TxMsgHandler;
import com.damon.localmsgtx.TxMsgModel;
import com.damon.localmsgtx.TxMsgSqlStore;
import com.damon.localmsgtx.TxMsgKafkaConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.transaction.support.TransactionSynchronization;
import org.springframework.transaction.support.TransactionSynchronizationAdapter;
import org.springframework.transaction.support.TransactionSynchronizationManager;
import org.springframework.util.Assert;

/**
 * Kafka transactional message client implementation
 * Ensuring eventual consistency between message sending and local transactions
 */
public class KafkaTxMsgClient implements ITxMsgClient {

    protected static final Logger logger = LoggerFactory.getLogger(KafkaTxMsgClient.class);

    private final TxMsgSqlStore txMsgSqlStore;
    private final TxMsgHandler txMsgHandler;


    public KafkaTxMsgClient(TxMsgKafkaConfig config) {
        Assert.notNull(config.getKafkaProducer(), "KafkaProducer cannot be null");
        Assert.notNull(config.getDataSource(), "DataSource cannot be null");
        Assert.hasText(config.getTxMsgTableName(), "Message storage table name cannot be empty");

        this.txMsgSqlStore = new TxMsgSqlStore(config.getDataSource(), config.getTxMsgTableName());
        this.txMsgHandler = new TxMsgHandler(config.getKafkaProducer(), this.txMsgSqlStore);
    }

    /**
     * Send transactional message
     * The message will be stored in the database first, and sent to Kafka after transaction commits
     *
     * @param topic   Message topic (non-null)
     * @param msgKey  Message key (non-null)
     * @param content Message content (non-null)
     * @return Message ID
     */
    @Override
    public Long sendTxMsg(String topic, String msgKey, String content) {
        // Parameter validation
        Assert.hasText(topic, "Message topic cannot be empty");
        Assert.hasText(content, "Message content cannot be empty");
        Assert.hasText(msgKey, "Message key cannot be empty");

        TxMsgModel txMsg = storeTxMsg(content, topic, msgKey);
        registerTransactionCallback(txMsg);
        return txMsg.getId();
    }

    /**
     * Store message to database (within local transaction)
     */
    private TxMsgModel storeTxMsg(String content, String topic, String msgKey) {
        // Check transaction status
        if (!TransactionSynchronizationManager.isActualTransactionActive()) {
            logger.error("Current operation is not within an active transaction, message sending consistency cannot be guaranteed");
        }


        TxMsgModel txMsg = txMsgSqlStore.insertTxMsg(content, topic, msgKey);
        logger.debug("Transactional message stored in database, msgId: {}", txMsg.getId());
        return txMsg;

    }

    /**
     * Register transaction synchronization callback, send message after transaction commits
     */
    private void registerTransactionCallback(TxMsgModel txMsg) {
        // Register post-transaction-commit callback
        TransactionSynchronization synchronization = new TransactionSynchronizationAdapter() {
            @Override
            public void afterCommit() {
                logger.debug("Transaction committed, preparing to send message, msgId: {}", txMsg.getId());
                txMsgHandler.sendMsg(txMsg);
            }

            @Override
            public void afterCompletion(int status) {
                if (status != TransactionSynchronization.STATUS_COMMITTED) {
                    logger.info("Transaction not committed (status: {}), no need to send message, msgId: {}", status, txMsg.getId());
                }
            }
        };

        TransactionSynchronizationManager.registerSynchronization(synchronization);
        logger.debug("Transaction synchronization callback registered, msgId: {}", txMsg.getId());
    }

    /**
     * Resend all unsent messages (compensation mechanism)
     */
    @Override
    public void resendFailedTxMsg() {
        logger.info("Starting message resend task");
        txMsgHandler.resendWaitingMessages();
    }

    /**
     * Clean up expired sent messages
     *
     * @param expireTime Expiration timestamp (milliseconds)
     */
    @Override
    public void cleanExpiredTxMsg(Long expireTime) {
        Assert.notNull(expireTime, "Expiration timestamp cannot be null");
        logger.info("Starting to clean up expired messages, expiration time: {}ms", expireTime);
        txMsgHandler.deleteExpiredSentMessages(expireTime);
    }
}