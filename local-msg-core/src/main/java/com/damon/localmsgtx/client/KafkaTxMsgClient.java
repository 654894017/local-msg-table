package com.damon.localmsgtx.client;

import com.damon.localmsgtx.config.TxMsgKafkaConfig;
import com.damon.localmsgtx.exception.TxMsgDuplicateKeyException;
import com.damon.localmsgtx.exception.TxMsgException;
import com.damon.localmsgtx.handler.TxMsgHandler;
import com.damon.localmsgtx.model.TxMsgModel;
import com.damon.localmsgtx.model.TxMsgStatusEnum;
import com.damon.localmsgtx.store.TxMsgSqlStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.transaction.support.TransactionSynchronization;
import org.springframework.transaction.support.TransactionSynchronizationAdapter;
import org.springframework.transaction.support.TransactionSynchronizationManager;
import org.springframework.util.Assert;

import java.util.concurrent.ExecutorService;

/**
 * Kafka transactional message client implementation
 * Ensuring eventual consistency between message sending and local transactions
 */
public class KafkaTxMsgClient implements ITxMsgClient {

    protected static final Logger logger = LoggerFactory.getLogger(KafkaTxMsgClient.class);

    private final TxMsgSqlStore txMsgSqlStore;
    private final TxMsgHandler txMsgHandler;
    private final boolean isAsyncSendMsg;
    private final ExecutorService asyncSendExecutor;

    public KafkaTxMsgClient(TxMsgKafkaConfig config) {
        Assert.notNull(config.getKafkaProducer(), "KafkaProducer cannot be null");
        Assert.notNull(config.getTxMsgSqlStore(), "DataSource cannot be null");
        this.txMsgSqlStore = config.getTxMsgSqlStore();
        this.txMsgHandler = new TxMsgHandler(config.getKafkaProducer(), this.txMsgSqlStore);
        this.isAsyncSendMsg = config.isAsyncSendMsg();
        this.asyncSendExecutor = config.getAsyncSendExecutor();
    }

    /**
     * Send transactional message
     * The message will be stored in the database first, and sent to Kafka after transaction commits
     *
     * @param msgKey  Message key (non-null)
     * @param content Message content (non-null)
     * @return Message ID
     */
    @Override
    public Long sendTxMsg(String msgKey, String content) throws TxMsgDuplicateKeyException, TxMsgException {
        // Parameter validation
        Assert.hasText(content, "Message content cannot be empty");
        Assert.hasText(msgKey, "Message key cannot be empty");

        TxMsgModel txMsg = storeTxMsg(content, msgKey);
        registerTransactionCallback(txMsg);
        return txMsg.getId();
    }

    /**
     * Store message to database (within local transaction)
     */
    private TxMsgModel storeTxMsg(String content, String msgKey) {
        // Check transaction status
        if (!TransactionSynchronizationManager.isActualTransactionActive()) {
            logger.error("Current operation is not within an active transaction, message sending consistency cannot be guaranteed");
        }


        TxMsgModel txMsg = txMsgSqlStore.insertTxMsg(content, msgKey);
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
                if (isAsyncSendMsg) {
                    asyncSendExecutor.submit(() -> txMsgHandler.sendMsg(txMsg));
                } else {
                    txMsgHandler.sendMsg(txMsg);
                }
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
        txMsgHandler.deleteExpiredSentMessages(expireTime, TxMsgStatusEnum.SENT);
    }
}