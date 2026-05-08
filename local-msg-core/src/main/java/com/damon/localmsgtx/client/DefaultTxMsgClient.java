package com.damon.localmsgtx.client;

import com.damon.localmsgtx.config.TxMsgConfig;
import com.damon.localmsgtx.exception.TxMsgException;
import com.damon.localmsgtx.handler.AbstractTxMsgHandler;
import com.damon.localmsgtx.model.TxMsgModel;
import com.damon.localmsgtx.model.TxMsgStatusEnum;
import com.damon.localmsgtx.utils.StrUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.transaction.support.TransactionSynchronization;
import org.springframework.transaction.support.TransactionSynchronizationManager;
import org.springframework.util.Assert;

import java.nio.charset.StandardCharsets;
import java.util.Optional;
import java.util.concurrent.ExecutorService;

/**
 * 事务消息客户端默认实现
 * <p>
 * 核心流程：
 * 1. 在本地事务内将消息持久化到数据库
 * 2. 注册事务提交后的回调，异步发送消息到MQ
 * 3. 若发送失败，由补偿任务定时重试
 * <p>
 * 通过此机制确保本地事务与消息发送的最终一致性。
 */
public class DefaultTxMsgClient implements ITxMsgClient {

    protected static final Logger logger = LoggerFactory.getLogger(DefaultTxMsgClient.class);

    /**
     * 单条消息最大字节数（1MB，Kafka默认限制）
     */
    private static final int MAX_MESSAGE_SIZE = 1048576;

    private final AbstractTxMsgHandler txMsgHandler;
    private final ExecutorService asyncSendExecutor;

    public DefaultTxMsgClient(TxMsgConfig config) {
        Assert.notNull(config.getAsyncSendExecutor(), "AsyncSendExecutor cannot be null");
        Assert.notNull(config.getTxMsgHandler(), "TxMsgHandler cannot be null");
        this.txMsgHandler = config.getTxMsgHandler();
        this.asyncSendExecutor = config.getAsyncSendExecutor();
    }

    @Override
    public Long sendTxMsg(String msgKey, String content) {
        return this.sendTxMsg(msgKey, StrUtil.EMPTY, content);
    }

    @Override
    public Long sendTxMsg(String msgKey, String msgTag, String content) {
        Assert.hasText(content, "Message content cannot be empty");
        Assert.hasText(msgKey, "Message key cannot be empty");
        Assert.isTrue(msgKey.length() <= 128, "Message key length cannot exceed 128 characters");
        if (StrUtil.isNotEmpty(msgTag)) {
            Assert.isTrue(msgTag.length() <= 128, "Message tag length cannot exceed 128 characters");
        }
        // 校验消息体大小
        int messageSize = content.getBytes(StandardCharsets.UTF_8).length;
        if (messageSize > MAX_MESSAGE_SIZE) {
            logger.warn("消息体大小 {} bytes 超过限制 {} bytes", messageSize, MAX_MESSAGE_SIZE);
            throw new TxMsgException("Message size exceeds limit of 1MB");
        }
        TxMsgModel txMsg = storeTxMsg(content, msgKey, Optional.ofNullable(msgTag).orElse(StrUtil.EMPTY));
        registerTransactionCallback(txMsg);
        return txMsg.getId();
    }

    /**
     * 将消息持久化到数据库（必须在活跃事务中执行）
     */
    private TxMsgModel storeTxMsg(String content, String msgKey, String msgTag) {
        if (!TransactionSynchronizationManager.isActualTransactionActive()) {
            throw new TxMsgException("当前操作不在活跃事务中，无法保证消息发送一致性");
        }
        TxMsgModel txMsg = txMsgHandler.saveMsg(content, msgKey, msgTag);
        logger.debug("事务消息已持久化, msgId: {}", txMsg.getId());
        return txMsg;
    }

    /**
     * 注册事务同步回调，在事务提交后异步发送消息
     */
    private void registerTransactionCallback(TxMsgModel txMsg) {
        TransactionSynchronization synchronization = new TransactionSynchronization() {
            @Override
            public void afterCommit() {
                logger.debug("事务已提交，准备发送消息, msgId: {}", txMsg.getId());
                asyncSendExecutor.submit(() -> txMsgHandler.sendMsg(txMsg));
            }

            @Override
            public void afterCompletion(int status) {
                if (status != TransactionSynchronization.STATUS_COMMITTED) {
                    logger.info("事务未提交(status: {})，消息不发送, msgId: {}", status, txMsg.getId());
                }
            }
        };
        TransactionSynchronizationManager.registerSynchronization(synchronization);
        logger.debug("事务同步回调已注册, msgId: {}", txMsg.getId());
    }

    @Override
    public void resendWaitingTxMsg() {
        logger.info("开始执行消息补偿重发任务");
        txMsgHandler.resendWaitingMessages();
    }

    @Override
    public void cleanExpiredTxMsg(Long expireTime) {
        Assert.notNull(expireTime, "过期时间戳不能为空");
        logger.info("开始清理过期消息, 过期时间: {}ms", expireTime);
        txMsgHandler.deleteExpiredSentMessages(expireTime, TxMsgStatusEnum.SENT);
    }
}
