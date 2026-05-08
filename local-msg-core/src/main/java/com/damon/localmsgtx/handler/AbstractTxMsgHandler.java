package com.damon.localmsgtx.handler;

import com.damon.localmsgtx.model.TxMsgModel;
import com.damon.localmsgtx.model.TxMsgStatusEnum;
import com.damon.localmsgtx.store.TxMsgSqlStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.Assert;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * 事务消息处理器抽象基类
 * <p>
 * 封装消息存储、补偿重发、过期清理等通用逻辑，
 * 子类只需实现具体的消息发送方法（单条/批量）。
 */
public abstract class AbstractTxMsgHandler {

    private static final Logger logger = LoggerFactory.getLogger(AbstractTxMsgHandler.class);

    /**
     * 过期消息批量删除大小
     */
    protected final int deleteBatchSize;

    /**
     * 事务消息持久化存储
     */
    protected final TxMsgSqlStore txMsgSqlStore;

    /**
     * 单次查询待发送消息的最大条数
     */
    protected final int fetchLimit;

    /**
     * 单次重发任务处理消息的上限（防止任务执行过久）
     */
    protected final int maxResendNumPerTask;

    /**
     * 批量发送异常后的休眠时间（秒）
     */
    protected final int exceptionSleep;

    /**
     * 最大重试次数，超过后标记为发送失败
     */
    protected final int maxRetryCount;

    protected AbstractTxMsgHandler(int deleteBatchSize, TxMsgSqlStore txMsgSqlStore, int fetchLimit,
                                   int maxResendNumPerTask, int exceptionSleep, int maxRetryCount) {
        Assert.isTrue(deleteBatchSize > 0, "deleteBatchSize must be greater than 0");
        Assert.notNull(txMsgSqlStore, "TxMsgSqlStore cannot be null");
        Assert.isTrue(fetchLimit > 0, "fetchLimit must be greater than 0");
        Assert.isTrue(maxResendNumPerTask > 0, "maxResendNumPerTask must be greater than 0");
        Assert.isTrue(exceptionSleep > 0, "exceptionSleep must be greater than 0");
        Assert.isTrue(maxRetryCount > 0, "maxRetryCount must be greater than 0");
        this.deleteBatchSize = deleteBatchSize;
        this.txMsgSqlStore = txMsgSqlStore;
        this.fetchLimit = fetchLimit;
        this.maxResendNumPerTask = maxResendNumPerTask;
        this.exceptionSleep = exceptionSleep;
        this.maxRetryCount = maxRetryCount;
    }

    /**
     * 保存事务消息到数据库
     *
     * @param content 消息内容
     * @param msgKey  消息唯一标识
     * @param msgTag  消息标签
     * @return 持久化后的消息模型
     */
    public TxMsgModel saveMsg(String content, String msgKey, String msgTag) {
        return txMsgSqlStore.insertTxMsg(content, msgKey, msgTag);
    }

    /**
     * 删除过期的已发送消息
     *
     * @param expireTime 过期时间戳（毫秒），该时间之前的消息将被删除
     * @param statusEnum 要清理的消息状态
     */
    public void deleteExpiredSentMessages(Long expireTime, TxMsgStatusEnum statusEnum) {
        Assert.notNull(expireTime, "过期时间戳不能为空");
        logger.info("开始清理过期消息, 过期时间: {}ms", expireTime);
        txMsgSqlStore.deleteExpiredSendedMsg(expireTime, deleteBatchSize, statusEnum);
        logger.info("过期消息清理完成");
    }

    /**
     * 发送单条事务消息
     * <p>
     * 事务提交后调用，若发送失败则标记为SEND_FAILED，等待补偿重发。
     *
     * @param txMsgModel 事务消息模型
     */
    public void sendMsg(TxMsgModel txMsgModel) {
        Assert.notNull(txMsgModel, "事务消息模型不能为空");
        Assert.hasText(txMsgModel.getTopic(), "消息主题不能为空");
        Assert.hasText(txMsgModel.getContent(), "消息内容不能为空");
        try {
            sendMessage(txMsgModel);
        } catch (Exception e) {
            logger.error("消息发送失败 [msgId: {}, topic: {}]", txMsgModel.getId(), txMsgModel.getTopic(), e);
            txMsgSqlStore.updateToSendFailed(txMsgModel.getId());
        }
    }

    /**
     * 补偿重发所有待发送的消息
     * <p>
     * 分页查询并批量发送，超过最大重试次数的消息标记为发送失败。
     * 单次任务处理消息数量受 maxResendNumPerTask 限制。
     */
    public void resendWaitingMessages() {
        int totalProcessed = 0;
        int currentFetchNum;
        Long maxId = 0L;

        do {
            if (totalProcessed >= maxResendNumPerTask) {
                logger.warn("单次重发任务已达处理上限: {}, 剩余消息将在下次任务处理", maxResendNumPerTask);
                break;
            }

            List<TxMsgModel> waitingMessages = txMsgSqlStore.getWaitingMessages(fetchLimit, maxId, maxRetryCount);
            currentFetchNum = waitingMessages.size();
            totalProcessed += currentFetchNum;

            if (currentFetchNum == 0) {
                logger.debug("无待重发消息");
                break;
            }

            // 过滤超过最大重试次数的消息，标记为发送失败
            List<TxMsgModel> retryableMessages = new ArrayList<>();
            for (TxMsgModel msg : waitingMessages) {
                if (msg.getRetryCount() >= maxRetryCount) {
                    logger.warn("消息超过最大重试次数，标记为发送失败 [msgId: {}, retryCount: {}, maxRetryCount: {}]",
                            msg.getId(), msg.getRetryCount(), maxRetryCount);
                    txMsgSqlStore.updateToSendFailed(msg.getId());
                } else {
                    retryableMessages.add(msg);
                }
            }

            logger.info("批量处理消息, 查询: {}, 可重试: {}, 累计处理: {}",
                    currentFetchNum, retryableMessages.size(), totalProcessed);

            if (!retryableMessages.isEmpty()) {
                doBatchSendMessages(retryableMessages);
            }

            maxId = waitingMessages.get(currentFetchNum - 1).getId();

        } while (currentFetchNum == fetchLimit);

        logger.info("重发任务完成, 本次共处理: {}", totalProcessed);
    }

    /**
     * 执行批量发送（带异常保护和休眠）
     */
    private void doBatchSendMessages(List<TxMsgModel> txMsgModels) {
        try {
            batchSendMessages(txMsgModels);
        } catch (Exception e) {
            logger.error("批量发送消息异常, 休眠 {}s", exceptionSleep, e);
            try {
                TimeUnit.SECONDS.sleep(exceptionSleep);
            } catch (InterruptedException ignored) {
                Thread.currentThread().interrupt();
            }
        }
    }

    /**
     * 发送单条消息（由子类实现具体MQ发送逻辑）
     *
     * @param txMsgModel 事务消息模型
     */
    protected abstract void sendMessage(TxMsgModel txMsgModel);

    /**
     * 批量发送消息（由子类实现具体MQ发送逻辑）
     *
     * @param txMsgModels 事务消息列表
     */
    protected abstract void batchSendMessages(List<TxMsgModel> txMsgModels);
}
