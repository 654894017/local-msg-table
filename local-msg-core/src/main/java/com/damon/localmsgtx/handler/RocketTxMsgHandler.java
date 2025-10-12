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
import org.apache.rocketmq.common.message.MessageBatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.Assert;

import java.util.List;
import java.util.stream.Collectors;

/**
 * Transactional message handler based on RocketMQ
 * Responsible for message sending, retry sending, and cleaning up expired sent messages
 */
public class RocketTxMsgHandler extends AbstractTxMsgHandler {
    private static final Logger logger = LoggerFactory.getLogger(RocketTxMsgHandler.class);
    private final DefaultMQProducer rocketProducer;

    /**
     * Full parameter constructor (recommended, supports custom configuration)
     *
     * @param rocketProducer      RocketMQ producer instance
     * @param txMsgSqlStore       Transactional message storage manager
     * @param fetchLimit          Number of pending messages to fetch in a single request
     * @param maxResendNumPerTask Maximum number of messages to resend in a single task
     * @param deleteBatchSize     Batch size for deletion
     */
    public RocketTxMsgHandler(DefaultMQProducer rocketProducer,
                              TxMsgSqlStore txMsgSqlStore,
                              int fetchLimit,
                              int maxResendNumPerTask,
                              int deleteBatchSize,
                              int exceptionSleep) {
        super(deleteBatchSize, txMsgSqlStore, fetchLimit, maxResendNumPerTask, exceptionSleep);
        // Parameter validation
        Assert.notNull(rocketProducer, "RocketMQ producer cannot be null");
        this.rocketProducer = rocketProducer;
    }

    /**
     * Simplified constructor (using default configuration)
     * Suitable for quick initialization with default values:
     * - Fetch 50 messages at a time
     * - Maximum 2000 messages per resend task
     * - Delete batch size of 200
     * - Exception sleep time of 5 seconds
     */
    public RocketTxMsgHandler(DefaultMQProducer rocketProducer,
                              TxMsgSqlStore txMsgSqlStore) {
        this(rocketProducer, txMsgSqlStore, 50, 2000, 200, 5);
    }

    /**
     * Actually execute single message sending logic
     */
    @Override
    protected void sendMessage(TxMsgModel txMsgModel) {
        String topic = txMsgModel.getTopic();
        Long msgId = txMsgModel.getId();
        try {
            // Build RocketMQ message
            Message message = convertToRocketMessages(txMsgModel);
            // Send message asynchronously
            rocketProducer.send(message, new SendCallback() {
                @Override
                public void onSuccess(SendResult sendResult) {
                    logger.debug("Message sent successfully [msgId: {}, topic: {}, messageId: {}, queueId: {}]",
                            msgId, topic, sendResult.getMsgId(), sendResult.getMessageQueue().getQueueId());

                    int updateRows = txMsgSqlStore.updateSendMsg(txMsgModel);
                    if (updateRows <= 0) {
                        logger.warn("Message status update failed, corresponding record not found [msgId: {}]", msgId);
                    }
                }

                @Override
                public void onException(Throwable e) {
                    logger.error("Message sending failed [msgId: {}, topic: {}]", msgId, topic, e);
                }
            });
        } catch (Exception e) {
            logger.error("Message sending execution exception [msgId: {}, topic: {}]", msgId, topic, e);
        }
    }

    /**
     * Actually execute batch message sending logic
     */
    @Override
    protected void batchSendMessages(List<TxMsgModel> txMsgModels) {
        if (ListUtils.isEmpty(txMsgModels)) {
            return;
        }
        // Group messages by topic (RocketMQ batch send requires same topic)
        // Here we assume all messages in batch have same topic, if not, need to group them
        String topic = txMsgModels.get(0).getTopic();
        List<Message> messages = txMsgModels.stream().map(this::convertToRocketMessages).toList();
        MessageBatch batch = MessageBatch.generateFromList(messages);
        batch.setBody(batch.encode());
        List<Long> msgIds = txMsgModels.stream().map(TxMsgModel::getId).collect(Collectors.toList());
        try {
            rocketProducer.send(batch);
        } catch (Exception e) {
            logger.error("RocketMQ topic:{}, batch message sending failed, failed message IDs: {}", topic, msgIds, e);
            throw new TxMsgException(e);
        }
        // Update status for successfully sent messages
        if (ListUtils.isNotEmpty(msgIds)) {
            int updateRows = txMsgSqlStore.batchUpdateSendMsg(msgIds);
            logger.info("Batch message status update completed, should update: {}, actually updated: {}",
                    msgIds.size(), updateRows);

            if (updateRows != msgIds.size()) {
                logger.warn("Some message status updates failed, expected to update: {}, actually updated: {}",
                        msgIds.size(), updateRows);
            }
        }
    }

    /**
     * Convert TxMsgModel list to RocketMQ Message list
     */
    private Message convertToRocketMessages(TxMsgModel model) {
        Message message = new Message(model.getTopic(), model.getContent().getBytes());
        message.setKeys(model.getMsgKey());
        if (StrUtil.isNotEmpty(model.getMsgTag())) {
            message.setTags(model.getMsgTag());
        }
        return message;

    }
}