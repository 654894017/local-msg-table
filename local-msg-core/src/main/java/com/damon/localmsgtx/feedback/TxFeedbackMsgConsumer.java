package com.damon.localmsgtx.feedback;

import com.alibaba.fastjson2.JSONObject;
import com.damon.localmsgtx.model.TxMsgFailed;
import com.damon.localmsgtx.model.TxMsgFeedback;
import com.damon.localmsgtx.model.TxMsgSuccess;
import com.damon.localmsgtx.store.TxMsgSqlStore;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.CollectionUtils;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

/**
 * Transaction Message Feedback Consumer
 * Processes message consumption result feedback from Kafka and updates message statuses
 */
public class TxFeedbackMsgConsumer implements Runnable {

    private static final Logger log = LoggerFactory.getLogger(TxFeedbackMsgConsumer.class);
    private static final int BATCH_PROCESS_SIZE = 500; // Batch processing size
    private static final Duration POLL_DURATION = Duration.ofMillis(1000); // Fixed polling duration

    private final TxMsgSqlStore txMsgSqlStore;
    private final KafkaConsumer<String, String> kafkaConsumer;
    private final AtomicBoolean isRunning = new AtomicBoolean(true); // Flag to control thread termination

    public TxFeedbackMsgConsumer(TxMsgSqlStore txMsgSqlStore,
                                 KafkaConsumer<String, String> kafkaConsumer) {
        this(txMsgSqlStore, kafkaConsumer, BATCH_PROCESS_SIZE);
    }

    public TxFeedbackMsgConsumer(TxMsgSqlStore txMsgSqlStore,
                                 KafkaConsumer<String, String> kafkaConsumer,
                                 int batchProcessSize) {
        // Parameter validation
        if (txMsgSqlStore == null) {
            throw new IllegalArgumentException("TransactionMessageSqlStore cannot be null");
        }
        if (kafkaConsumer == null) {
            throw new IllegalArgumentException("KafkaConsumer cannot be null");
        }

        if (batchProcessSize <= 0) {
            throw new IllegalArgumentException("batchProcessSize must be greater than 0");
        }

        this.txMsgSqlStore = txMsgSqlStore;
        this.kafkaConsumer = kafkaConsumer;
    }


    @Override
    public void run() {
        log.info("Transaction feedback message consumer started");

        while (true) {
            try {
                ConsumerRecords<String, String> records = kafkaConsumer.poll(POLL_DURATION);
                if (records.isEmpty()) {
                    continue;
                }

                // Process messages and update statuses in batches
                processRecords(records);
                // Synchronously commit offsets
                kafkaConsumer.commitSync();
            } catch (Exception e) {
                log.error("Error occurred while processing feedback messages", e);
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException ie) {
                    // Restore interrupt status
                    Thread.currentThread().interrupt();
                }
            }
        }
    }

    /**
     * Processes message records and updates statuses in batches
     */
    private void processRecords(ConsumerRecords<String, String> records) {
        List<TxMsgFeedback> messageFeedbacks = parseEventsFromRecords(records);
        if (CollectionUtils.isEmpty(messageFeedbacks)) {
            return;
        }

        // Group by success/failure
        Map<Boolean, List<TxMsgFeedback>> feedbackMessageGroups = messageFeedbacks.stream()
                .collect(Collectors.partitioningBy(TxMsgFeedback::isSuccess));

        // Process successful messages - batch processing
        processSuccessFeedbacks(feedbackMessageGroups.get(true));
        // Process failed messages - batch processing
        processFailedFeedbacks(feedbackMessageGroups.get(false));
    }

    /**
     * Processes successful feedback messages
     */
    private void processSuccessFeedbacks(List<TxMsgFeedback> successFeedbacks) {
        if (CollectionUtils.isEmpty(successFeedbacks)) {
            return;
        }

        // Convert to business objects
        List<TxMsgSuccess> successMessages = successFeedbacks.stream()
                .map(feedback -> new TxMsgSuccess(feedback.getMsgKey(), feedback.getProcessTime()))
                .collect(Collectors.toList());

        // Process in batches to avoid excessive data volume in single operation
        for (List<TxMsgSuccess> batch : splitIntoBatches(successMessages, BATCH_PROCESS_SIZE)) {
            int updated = txMsgSqlStore.batchMarkConsumerSuccess(batch);
            log.info("Processed {} success feedbacks, updated {} records", batch.size(), updated);
        }
    }

    /**
     * Processes failed feedback messages
     */
    private void processFailedFeedbacks(List<TxMsgFeedback> failedFeedbacks) {
        if (CollectionUtils.isEmpty(failedFeedbacks)) {
            return;
        }

        // Convert to business objects
        List<TxMsgFailed> failedMessages = failedFeedbacks.stream()
                .map(feedback -> new TxMsgFailed(
                        feedback.getMsgKey(),
                        feedback.getErrorMsg(),
                        feedback.getProcessTime()
                )).collect(Collectors.toList());

        // Process in batches
        for (List<TxMsgFailed> batch : splitIntoBatches(failedMessages, BATCH_PROCESS_SIZE)) {
            int updated = txMsgSqlStore.batchMarkConsumerFailed(batch);
            log.info("Processed {} failed feedbacks, updated {} records", batch.size(), updated);
        }
    }

    /**
     * Parses Kafka records into message feedback objects
     */
    private List<TxMsgFeedback> parseEventsFromRecords(ConsumerRecords<String, String> records) {
        List<TxMsgFeedback> messageFeedbacks = new ArrayList<>(records.count());
        for (ConsumerRecord<String, String> record : records) {
            try {
                TxMsgFeedback feedback = JSONObject.parseObject(record.value(), TxMsgFeedback.class);
                messageFeedbacks.add(feedback);
            } catch (Exception e) {
                log.error("Failed to parse feedback message, offset: {}, content: {}", record.offset(), record.value());
            }
        }

        return messageFeedbacks;
    }

    /**
     * Splits a list into batches of specified size
     */
    private <T> List<List<T>> splitIntoBatches(List<T> list, int batchSize) {
        List<List<T>> batches = new ArrayList<>();
        for (int i = 0; i < list.size(); i += batchSize) {
            int end = Math.min(i + batchSize, list.size());
            batches.add(list.subList(i, end));
        }
        return batches;
    }

}
