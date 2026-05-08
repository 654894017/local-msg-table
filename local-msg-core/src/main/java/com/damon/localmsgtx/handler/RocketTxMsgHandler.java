package com.damon.localmsgtx.handler;

import com.damon.localmsgtx.exception.TxMsgException;
import com.damon.localmsgtx.model.TxMsgModel;
import com.damon.localmsgtx.store.TxMsgSqlStore;
import com.damon.localmsgtx.utils.ListUtils;
import com.damon.localmsgtx.utils.StrUtil;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendCallback;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.Assert;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

/**
 * 基于RocketMQ的事务消息处理器
 * <p>
 * 支持单条异步发送和批量发送，发送失败时累加重试次数。
 */
public class RocketTxMsgHandler extends AbstractTxMsgHandler {

    private static final Logger logger = LoggerFactory.getLogger(RocketTxMsgHandler.class);

    private final DefaultMQProducer rocketProducer;

    /**
     * 全参构造器
     *
     * @param rocketProducer      RocketMQ生产者实例
     * @param txMsgSqlStore       事务消息存储
     * @param fetchLimit          单次查询待发送消息条数
     * @param maxResendNumPerTask 单次重发任务最大处理消息数
     * @param deleteBatchSize     过期消息批量删除大小
     * @param exceptionSleep      异常后休眠时间（秒）
     * @param maxRetryCount       最大重试次数
     */
    public RocketTxMsgHandler(DefaultMQProducer rocketProducer,
                              TxMsgSqlStore txMsgSqlStore,
                              int fetchLimit,
                              int maxResendNumPerTask,
                              int deleteBatchSize,
                              int exceptionSleep,
                              int maxRetryCount) {
        super(deleteBatchSize, txMsgSqlStore, fetchLimit, maxResendNumPerTask, exceptionSleep, maxRetryCount);
        Assert.notNull(rocketProducer, "RocketMQ producer cannot be null");
        this.rocketProducer = rocketProducer;
    }

    /**
     * 简化构造器（使用默认配置）
     * <p>
     * 默认值：fetchLimit=50, maxResendNumPerTask=2000, deleteBatchSize=200,
     * exceptionSleep=5s, maxRetryCount=5
     */
    public RocketTxMsgHandler(DefaultMQProducer rocketProducer,
                              TxMsgSqlStore txMsgSqlStore) {
        this(rocketProducer, txMsgSqlStore, 50, 2000, 200, 5, 5);
    }

    /**
     * 单条消息发送（异步回调）
     * <p>
     * 发送成功更新状态为已发送，发送失败累加重试次数。
     */
    @Override
    protected void sendMessage(TxMsgModel txMsgModel) {
        String topic = txMsgModel.getTopic();
        Long msgId = txMsgModel.getId();
        try {
            Message message = convertToRocketMessage(txMsgModel);
            rocketProducer.send(message, new SendCallback() {
                @Override
                public void onSuccess(SendResult sendResult) {
                    logger.debug("消息发送成功 [msgId: {}, topic: {}, messageId: {}, queueId: {}]",
                            msgId, topic, sendResult.getMsgId(), sendResult.getMessageQueue().getQueueId());
                    int updateRows = txMsgSqlStore.updateSendMsg(txMsgModel);
                    if (updateRows <= 0) {
                        logger.warn("消息状态更新失败，记录不存在 [msgId: {}]", msgId);
                    }
                }

                @Override
                public void onException(Throwable e) {
                    logger.error("消息发送失败 [msgId: {}, topic: {}]", msgId, topic, e);
                    txMsgSqlStore.incrementRetryCount(msgId);
                }
            });
        } catch (Exception e) {
            logger.error("消息发送异常 [msgId: {}, topic: {}]", msgId, topic, e);
            txMsgSqlStore.incrementRetryCount(msgId);
        }
    }

    /**
     * 批量消息发送
     * <p>
     * 通过CompletableFuture收集异步发送结果，然后批量更新成功消息状态，
     * 失败消息累加重试次数。
     */
    @Override
    protected void batchSendMessages(List<TxMsgModel> txMsgModels) {
        if (ListUtils.isEmpty(txMsgModels)) {
            return;
        }

        List<CompletableFuture<MsgSendResult>> futures = txMsgModels.stream().map(msg -> {
            Message message = convertToRocketMessage(msg);
            CompletableFuture<MsgSendResult> future = new CompletableFuture<>();
            try {
                rocketProducer.send(message, new SendCallback() {
                    @Override
                    public void onSuccess(SendResult sendResult) {
                        future.complete(MsgSendResult.success(msg.getId()));
                    }

                    @Override
                    public void onException(Throwable e) {
                        logger.error("批量消息发送失败 [msgId: {}, topic: {}]", msg.getId(), message.getTopic(), e);
                        future.complete(MsgSendResult.failure(msg.getId()));
                    }
                });
            } catch (Throwable e) {
                throw new TxMsgException(e);
            }
            return future;
        }).collect(Collectors.toList());

        // 等待所有发送完成
        List<MsgSendResult> results = futures.stream().map(CompletableFuture::join).collect(Collectors.toList());

        List<Long> sentSuccessMsgIds = results.stream()
                .filter(MsgSendResult::isSuccess)
                .map(MsgSendResult::getMsgId)
                .collect(Collectors.toList());

        List<Long> sentFailedMsgIds = results.stream()
                .filter(result -> !result.isSuccess())
                .map(MsgSendResult::getMsgId)
                .collect(Collectors.toList());

        // 失败消息累加重试次数
        sentFailedMsgIds.forEach(txMsgSqlStore::incrementRetryCount);

        // 批量更新成功消息状态
        if (ListUtils.isNotEmpty(sentSuccessMsgIds)) {
            int updateRows = txMsgSqlStore.batchUpdateSendMsg(sentSuccessMsgIds);
            logger.info("批量更新消息状态完成, 应更新: {}, 实际更新: {}", sentSuccessMsgIds.size(), updateRows);
            if (updateRows != sentSuccessMsgIds.size()) {
                logger.warn("部分消息状态更新失败, 预期: {}, 实际: {}", sentSuccessMsgIds.size(), updateRows);
            }
        }
    }

    /**
     * 将事务消息模型转换为RocketMQ Message
     */
    private Message convertToRocketMessage(TxMsgModel model) {
        Message message = new Message(model.getTopic(), model.getContent().getBytes());
        message.setKeys(model.getMsgKey());
        if (StrUtil.isNotEmpty(model.getMsgTag())) {
            message.setTags(model.getMsgTag());
        }
        return message;
    }

    /**
     * 消息发送结果（用于异步回调结果收集）
     */
    public static class MsgSendResult {
        private final boolean success;
        private final Long msgId;

        public MsgSendResult(boolean success, Long msgId) {
            this.success = success;
            this.msgId = msgId;
        }

        public static MsgSendResult success(Long msgId) {
            return new MsgSendResult(true, msgId);
        }

        public static MsgSendResult failure(Long msgId) {
            return new MsgSendResult(false, msgId);
        }

        public boolean isSuccess() {
            return success;
        }

        public Long getMsgId() {
            return msgId;
        }
    }
}
