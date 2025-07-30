package com.damon.localmsgtx;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.Assert;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.stream.Collectors;

/**
 * Transactional message handler
 * Responsible for message sending, retry sending, and cleaning up expired sent messages
 */
public class TxMsgHandler {
    private static final Logger logger = LoggerFactory.getLogger(TxMsgHandler.class);

    /**
     * Maximum number of pending messages to fetch in a single request
     */
    private final int fetchLimit;
    /**
     * Maximum number of messages to process in a single resend task (to prevent tasks from being too long)
     */
    private final int maxResendNumPerTask;
    /**
     * Batch size for deleting expired messages in a single operation
     */
    private final int deleteBatchSize;

    private final KafkaProducer<String, String> kafkaProducer;
    private final TxMsgSqlStore txMsgSqlStore;

    /**
     * Full parameter constructor (recommended, supports custom configuration)
     *
     * @param kafkaProducer       Kafka producer instance
     * @param txMsgSqlStore       Transactional message storage manager
     * @param fetchLimit          Number of pending messages to fetch in a single request
     * @param maxResendNumPerTask Maximum number of messages to resend in a single task
     * @param deleteBatchSize     Batch size for deletion
     * @param batchSendSize       Batch size for sending
     */
    public TxMsgHandler(KafkaProducer<String, String> kafkaProducer,
                        TxMsgSqlStore txMsgSqlStore,
                        int fetchLimit,
                        int maxResendNumPerTask,
                        int deleteBatchSize,
                        int batchSendSize) {
        // Parameter validation
        Assert.notNull(kafkaProducer, "KafkaProducer cannot be null");
        Assert.notNull(txMsgSqlStore, "TxMsgSqlStore cannot be null");
        Assert.isTrue(fetchLimit > 0, "Fetch limit must be greater than 0");
        Assert.isTrue(maxResendNumPerTask > 0, "Maximum resend number per task must be greater than 0");
        Assert.isTrue(deleteBatchSize > 0, "Delete batch size must be greater than 0");
        Assert.isTrue(batchSendSize > 0, "Batch send size must be greater than 0");

        this.kafkaProducer = kafkaProducer;
        this.txMsgSqlStore = txMsgSqlStore;
        this.fetchLimit = fetchLimit;
        this.maxResendNumPerTask = maxResendNumPerTask;
        this.deleteBatchSize = deleteBatchSize;
    }

    /**
     * Simplified constructor (using default configuration)
     * Suitable for quick initialization with default values:
     * - Fetch 50 messages at a time
     * - Maximum 2000 messages per resend task
     * - Delete batch size of 200
     * - Batch send size of 50
     */
    public TxMsgHandler(KafkaProducer<String, String> kafkaProducer,
                        TxMsgSqlStore txMsgSqlStore) {
        this(kafkaProducer, txMsgSqlStore, 50, 2000, 200, 50);
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

        doSendMessage(txMsgModel);
    }

    /**
     * Batch send transactional messages to Kafka
     *
     * @param txMsgModels List of transactional message models (cannot be null and cannot contain null elements)
     */
    public void batchSendMsgs(List<TxMsgModel> txMsgModels) {
        Assert.notNull(txMsgModels, "Transactional message model list cannot be null");
        Assert.noNullElements(txMsgModels.toArray(), "Transactional message model list cannot contain null elements");
        doBatchSendMessages(txMsgModels);
    }

    /**
     * Resend all messages in "waiting to send" status
     * Used for compensation mechanism to ensure unsent messages are retried
     */
    public void resendWaitingMessages() {
        int totalProcessed = 0;
        int currentFetchNum;

        // Loop to fetch and process messages until no more messages or reaching the single task processing limit
        do {
            // Avoid exceeding the maximum processing limit
            if (totalProcessed >= maxResendNumPerTask) {
                logger.warn("Single resend task has reached the maximum processing limit: {}, remaining messages will be processed in the next task", maxResendNumPerTask);
                break;
            }

            // Fetch pending messages
            List<TxMsgModel> waitingMessages = txMsgSqlStore.getWaitingMessages(fetchLimit);
            currentFetchNum = waitingMessages.size();
            totalProcessed += currentFetchNum;

            if (currentFetchNum == 0) {
                logger.debug("No messages pending resend");
                break;
            }

            logger.info("Starting to process batch messages, count: {}, total processed: {}", currentFetchNum, totalProcessed);

            // Batch send messages
            batchSendMsgs(waitingMessages);

        } while (currentFetchNum == fetchLimit); // If this fetch is full, there may be more messages

        logger.info("Resend task completed, total messages processed in this run: {}", totalProcessed);
    }

    /**
     * Delete expired sent messages
     *
     * @param expireTime Expiration timestamp (milliseconds), sent messages with timestamps less than or equal to this time will be deleted
     */
    public void deleteExpiredSentMessages(Long expireTime) {
        Assert.notNull(expireTime, "Expiration timestamp cannot be null");
        logger.info("Starting to clean up expired sent messages, expiration time: {}ms", expireTime);
        txMsgSqlStore.deleteExpiredSendedMsg(expireTime, deleteBatchSize);
        logger.info("Cleanup of expired sent messages completed");
    }

    /**
     * Actually execute single message sending logic
     */
    private void doSendMessage(TxMsgModel txMsgModel) {
        String topic = txMsgModel.getTopic();
        String msgKey = txMsgModel.getMsgKey();
        String content = txMsgModel.getContent();
        Long msgId = txMsgModel.getId();
        try {
            // Build Kafka message
            ProducerRecord<String, String> record = new ProducerRecord<>(topic, msgKey, content);
            RecordMetadata metadata = kafkaProducer.send(record).get();
            logger.info("Message sent successfully [msgId: {}, topic: {}, partition: {}, offset: {}]",
                    msgId, metadata.topic(), metadata.partition(), metadata.offset());
            // Update message status to "sent" after successful sending
            int updateRows = txMsgSqlStore.updateSendMsg(txMsgModel);
            if (updateRows <= 0) {
                logger.warn("Message status update failed, corresponding record not found [msgId: {}]", msgId);
            }
        } catch (Exception e) {
            logger.error("Message sending execution exception [msgId: {}, topic: {}]", msgId, topic, e);
        }
    }

    /**
     * Actually execute batch message sending logic
     */
    private void doBatchSendMessages(List<TxMsgModel> txMsgModels) {
        try {
            // Batch send messages and collect results
            List<Future<RecordMetadata>> futures = txMsgModels.stream()
                    .map(model -> {
                        ProducerRecord<String, String> record =
                                new ProducerRecord<>(model.getTopic(), model.getMsgKey(), model.getContent());
                        return kafkaProducer.send(record);
                    }).collect(Collectors.toList());

            // Process sending results and collect successful message IDs
            List<Long> successMsgIds = new ArrayList<>();
            for (int i = 0; i < futures.size(); i++) {
                TxMsgModel model = txMsgModels.get(i);
                try {
                    RecordMetadata metadata = futures.get(i).get();
                    logger.info("Message sent successfully [msgId: {}, topic: {}, partition: {}, offset: {}]",
                            model.getId(), metadata.topic(), metadata.partition(), metadata.offset());
                    successMsgIds.add(model.getId());
                } catch (InterruptedException | ExecutionException e) {
                    logger.error("Message sending execution exception [msgId: {}, topic: {}]", model.getId(), model.getTopic(), e);
                    // Preserve interrupt status
                    if (e instanceof InterruptedException) {
                        Thread.currentThread().interrupt();
                    }
                }
            }

            // Batch update status of successfully sent messages
            if (!successMsgIds.isEmpty()) {
                int updateRows = txMsgSqlStore.batchUpdateSendMsg(successMsgIds);
                logger.info("Batch message status update completed, should update: {}, actually updated: {}", successMsgIds.size(), updateRows);
                if (updateRows != successMsgIds.size()) {
                    logger.warn("Some message status updates failed, expected to update: {}, actually updated: {}", successMsgIds.size(), updateRows);
                }
            }
        } catch (Exception e) {
            logger.error("Batch message sending execution exception", e);
        }
    }

}