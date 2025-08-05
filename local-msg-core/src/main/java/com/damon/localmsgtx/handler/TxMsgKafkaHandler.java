package com.damon.localmsgtx.handler;

import com.alibaba.fastjson2.JSONObject;
import com.damon.localmsgtx.utils.GenericsUtils;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.CollectionUtils;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

public abstract class TxMsgKafkaHandler<T> implements Runnable {
    private static final Duration POLL_DURATION = Duration.ofMillis(1000); // Fixed polling duration
    private final Logger log = LoggerFactory.getLogger(TxMsgKafkaHandler.class);
    private final KafkaConsumer<String, String> kafkaConsumer;

    public TxMsgKafkaHandler(KafkaConsumer<String, String> kafkaConsumer) {
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
                processMsgs(records);
            } catch (Exception e) {
                log.error("Error occurred while processing feedback messages", e);
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException ie) {
                    // Restore interrupt status
                    Thread.currentThread().interrupt();
                }
            } finally {
                // Synchronously commit offsets
                kafkaConsumer.commitSync();
            }
        }
    }

    private List<T> parseMsg(ConsumerRecords<String, String> records) {
        List<T> msgs = new ArrayList<>(records.count());
        for (ConsumerRecord<String, String> record : records) {
            try {
                T msg = JSONObject.parseObject(record.value(), getMsgType());
                msgs.add(msg);
            } catch (Exception e) {
                log.error("Failed to parse tx message, offset: {}, content: {}", record.offset(), record.value());
            }
        }
        return msgs;
    }

    private void processMsgs(ConsumerRecords<String, String> records) {
        List<T> msgs = parseMsg(records);
        if (CollectionUtils.isEmpty(msgs)) {
            return;
        }
        process(msgs);
    }

    public abstract void process(List<T> msgs);

    private Class<T> getMsgType() {
        return GenericsUtils.getSuperClassGenricType(this.getClass(), 0);
    }
}
