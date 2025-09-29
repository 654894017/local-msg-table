package com.damon.localmsgtx.client;

import com.damon.localmsgtx.config.TxMsgKafkaConfig;
import com.damon.localmsgtx.exception.TxMsgException;
import com.damon.localmsgtx.handler.TxMsgHandler;
import com.damon.localmsgtx.model.TxMsgModel;
import com.damon.localmsgtx.model.TxMsgStatusEnum;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.transaction.support.TransactionSynchronization;
import org.springframework.transaction.support.TransactionSynchronizationAdapter;
import org.springframework.transaction.support.TransactionSynchronizationManager;
import org.springframework.util.Assert;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.ExecutorService;

/**
 * Kafka transactional message client implementation
 * Ensuring eventual consistency between message sending and local transactions
 */
public class KafkaTxMsgClient implements ITxMsgClient {

    protected static final Logger logger = LoggerFactory.getLogger(KafkaTxMsgClient.class);
    private static final int MAX_MESSAGE_SIZE = 1048576;
    private final TxMsgHandler txMsgHandler;
    private final ExecutorService asyncSendExecutor;

    public KafkaTxMsgClient(TxMsgKafkaConfig config) {
        Assert.notNull(config.getKafkaProducer(), "KafkaProducer cannot be null");
        Assert.notNull(config.getTxMsgSqlStore(), "DataSource cannot be null");
        this.txMsgHandler = new TxMsgHandler(config.getKafkaProducer(), config.getTxMsgSqlStore());
        this.asyncSendExecutor = config.getAsyncSendExecutor();
    }

    /**
     * Send transactional message
     * The message will be stored in the database first, and sent to Kafka after transaction commits
     * <p>
     * Single message size limit: 1MB (Kafka default limit)
     * If message size exceeds this limit, a TxMsgException will be thrown
     *
     * @param msgKey  Message key (non-null)
     * @param content Message content (non-null)
     * @return Message ID
     */
    @Override
    public Long sendTxMsg(String msgKey, String content) throws TxMsgException {
        // Parameter validation
        Assert.hasText(content, "Message content cannot be empty");
        Assert.hasText(msgKey, "Message key cannot be empty");
        // 检查消息大小是否超过 Kafka 默认限制
        int messageSize = content.getBytes(StandardCharsets.UTF_8).length;
        if (messageSize > MAX_MESSAGE_SIZE) {
            logger.warn("Message size {} bytes exceeds Kafka default limit {} bytes", messageSize, MAX_MESSAGE_SIZE);
            throw new TxMsgException("Message size exceeds Kafka default limit of 1MB");
        }
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

        TxMsgModel txMsg = txMsgHandler.saveMsg(content, msgKey);
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
                asyncSendExecutor.submit(() -> txMsgHandler.sendMsg(txMsg));
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
    public void resendWaitingTxMsg(String shardTailNumber) {
        logger.info("Starting message resend task");
        txMsgHandler.resendWaitingMessages(shardTailNumber);
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
