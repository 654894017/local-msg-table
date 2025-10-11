package com.damon.localmsgtx.handler;

import com.damon.localmsgtx.model.TxMsgModel;
import com.damon.localmsgtx.store.TxMsgSqlStore;
import com.damon.localmsgtx.utils.ListUtils;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.Assert;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Transactional message handler
 * Responsible for message sending, retry sending, and cleaning up expired sent messages
 */
public class KafkaTxMsgHandler extends AbstractTxMsgHandler {
    private static final Logger logger = LoggerFactory.getLogger(KafkaTxMsgHandler.class);


    private final KafkaProducer<String, String> kafkaProducer;

    /**
     * Full parameter constructor (recommended, supports custom configuration)
     *
     * @param kafkaProducer       Kafka producer instance
     * @param txMsgSqlStore       Transactional message storage manager
     * @param fetchLimit          Number of pending messages to fetch in a single request
     * @param maxResendNumPerTask Maximum number of messages to resend in a single task
     * @param deleteBatchSize     Batch size for deletion
     */
    public KafkaTxMsgHandler(KafkaProducer<String, String> kafkaProducer,
                             TxMsgSqlStore txMsgSqlStore,
                             int fetchLimit,
                             int maxResendNumPerTask,
                             int deleteBatchSize) {
        super(deleteBatchSize, txMsgSqlStore, fetchLimit, maxResendNumPerTask);
        // Parameter validation
        Assert.notNull(kafkaProducer, "KafkaProducer cannot be null");
        this.kafkaProducer = kafkaProducer;
    }

    /**
     * Simplified constructor (using default configuration)
     * Suitable for quick initialization with default values:
     * - Fetch 50 messages at a time
     * - Maximum 2000 messages per resend task
     * - Delete batch size of 200
     */
    public KafkaTxMsgHandler(KafkaProducer<String, String> kafkaProducer,
                             TxMsgSqlStore txMsgSqlStore) {
        this(kafkaProducer, txMsgSqlStore, 50, 2000, 200);
    }

    /**
     * Actually execute single message sending logic
     */
    @Override
    protected void sendMessage(TxMsgModel txMsgModel) {
        String topic = txMsgModel.getTopic();
        String msgKey = txMsgModel.getMsgKey();
        String content = txMsgModel.getContent();
        Long msgId = txMsgModel.getId();
        try {
            // Build Kafka message
            ProducerRecord<String, String> record = new ProducerRecord<>(topic, msgKey, content);
            kafkaProducer.send(record, (metadata, exception) -> {
                if (exception == null) {
                    logger.debug("Message sent successfully [msgId: {}, topic: {}, partition: {}, offset: {}]",
                            msgId, metadata.topic(), metadata.partition(), metadata.offset());
                    int updateRows = txMsgSqlStore.updateSendMsg(txMsgModel);
                    if (updateRows <= 0) {
                        logger.warn("Message status update failed, corresponding record not found [msgId: {}]", msgId);
                    }
                } else {
                    logger.error("Message sending failed [msgId: {}, topic: {}]", msgId, topic, exception);
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
        // Process sending results and collect successful message IDs
        final List<Long> successMsgIds = Collections.synchronizedList(new ArrayList<>(txMsgModels.size()));
        final List<Long> failedMsgIds = Collections.synchronizedList(new ArrayList<>());
        // Use callback for async processing
        txMsgModels.forEach(model -> {
            ProducerRecord<String, String> record = new ProducerRecord<>(model.getTopic(), model.getMsgKey(), model.getContent());
            kafkaProducer.send(record, (metadata, exception) -> {
                if (exception == null) {
                    logger.debug("Batch message sent successfully [msgId: {}, topic: {}]", model.getId(), model.getTopic());
                    successMsgIds.add(model.getId());
                } else {
                    logger.error("Batch message sending failed [msgId: {}, topic: {}]", model.getId(), model.getTopic(), exception);
                    failedMsgIds.add(model.getId());
                }
            });
        });
        // Wait for all messages to be sent
        kafkaProducer.flush();
        if (ListUtils.isNotEmpty(successMsgIds)) {
            // Batch update status of successfully sent messages
            int updateRows = txMsgSqlStore.batchUpdateSendMsg(successMsgIds);
            logger.info("Batch message status update completed, should update: {}, actually updated: {}", successMsgIds.size(), updateRows);
            if (updateRows != successMsgIds.size()) {
                logger.warn("Some message status updates failed, expected to update: {}, actually updated: {}", successMsgIds.size(), updateRows);
            }
        }

        if (ListUtils.isNotEmpty(failedMsgIds)) {
            String topic = txMsgModels.get(0).getTopic();
            logger.error("Kafka topic:{}, batch message sending failed, failed message IDs: {}", topic, failedMsgIds);
        }


    }
}
