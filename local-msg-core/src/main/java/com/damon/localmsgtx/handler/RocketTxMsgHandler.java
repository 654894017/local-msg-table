package com.damon.localmsgtx.handler;

import com.damon.localmsgtx.exception.TxMsgException;
import com.damon.localmsgtx.model.TxMsgModel;
import com.damon.localmsgtx.store.TxMsgSqlStore;
import com.damon.localmsgtx.utils.ListUtils;
import com.damon.localmsgtx.utils.StrUtil;
import org.apache.commons.lang3.exception.ExceptionUtils;
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
 * 支持单条异步发送和批量发送，通过充血模型完成状态变更后统一持久化。
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
     * 发送成功标记为已发送并持久化，发送失败累加重试次数并持久化。
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
                    txMsgModel.markAsSent();
                    int rows = txMsgSqlStore.save(txMsgModel);
                    if (rows <= 0) {
                        logger.warn("消息状态保存失败（版本冲突）[msgId: {}]", msgId);
                    }
                }

                @Override
                public void onException(Throwable e) {
                    logger.error("消息发送失败 [msgId: {}, topic: {}]", msgId, topic, e);
                    txMsgModel.incrementRetry(e.getMessage());
                    txMsgSqlStore.save(txMsgModel);
                }
            });
        } catch (Exception e) {
            logger.error("消息发送异常 [msgId: {}, topic: {}]", msgId, topic, e);
            txMsgModel.incrementRetry(e.getMessage());
            txMsgSqlStore.save(txMsgModel);
        }
    }

    /**
     * 批量消息发送
     * <p>
     * 通过CompletableFuture收集异步发送结果，成功消息标记为已发送，
     * 失败消息累加重试次数，统一通过 save 持久化。
     */
    @Override
    protected void batchSendMessages(List<TxMsgModel> txMsgModels) {
        if (ListUtils.isEmpty(txMsgModels)) {
            return;
        }

        List<CompletableFuture<SendResultHolder>> futures = txMsgModels.stream().map(msg -> {
            Message message = convertToRocketMessage(msg);
            CompletableFuture<SendResultHolder> future = new CompletableFuture<>();
            try {
                rocketProducer.send(message, new SendCallback() {
                    @Override
                    public void onSuccess(SendResult sendResult) {
                        future.complete(SendResultHolder.success(msg));
                    }

                    @Override
                    public void onException(Throwable e) {
                        logger.error("批量消息发送失败 [msgId: {}, topic: {}]", msg.getId(), message.getTopic(), e);
                        future.complete(SendResultHolder.failure(msg, ExceptionUtils.getStackTrace(e)));
                    }
                });
            } catch (Throwable e) {
                throw new TxMsgException(e);
            }
            return future;
        }).collect(Collectors.toList());

        // 等待所有发送完成
        List<SendResultHolder> results = futures.stream().map(CompletableFuture::join).collect(Collectors.toList());

        // 成功消息标记为已发送并持久化
        results.stream().filter(SendResultHolder::isSuccess).forEach(r -> {
            r.getModel().markAsSent();
            txMsgSqlStore.save(r.getModel());
        });

        // 失败消息累加重试次数并持久化
        results.stream().filter(r -> !r.isSuccess()).forEach(r -> {
            r.getModel().incrementRetry(r.getFailReason());
            txMsgSqlStore.save(r.getModel());
        });
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
     * 发送结果持有者（用于异步回调结果收集）
     */
    private static class SendResultHolder {
        private final boolean success;
        private final TxMsgModel model;
        private final String failReason;

        private SendResultHolder(boolean success, TxMsgModel model, String failReason) {
            this.success = success;
            this.model = model;
            this.failReason = failReason;
        }

        static SendResultHolder success(TxMsgModel model) {
            return new SendResultHolder(true, model, null);
        }

        static SendResultHolder failure(TxMsgModel model, String failReason) {
            return new SendResultHolder(false, model, failReason);
        }

        boolean isSuccess() {
            return success;
        }

        TxMsgModel getModel() {
            return model;
        }

        String getFailReason() {
            return failReason;
        }
    }
}
