package com.damon.localmsgtx.handler;

import com.damon.localmsgtx.exception.TxMsgException;
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
 * 基于Kafka的事务消息处理器
 * <p>
 * 支持单条异步发送和批量发送，发送失败时累加重试次数。
 */
public class KafkaTxMsgHandler extends AbstractTxMsgHandler {

    private static final Logger logger = LoggerFactory.getLogger(KafkaTxMsgHandler.class);

    private final KafkaProducer<String, String> kafkaProducer;

    /**
     * 全参构造器
     *
     * @param kafkaProducer       Kafka生产者实例
     * @param txMsgSqlStore       事务消息存储
     * @param fetchLimit          单次查询待发送消息条数
     * @param maxResendNumPerTask 单次重发任务最大处理消息数
     * @param deleteBatchSize     过期消息批量删除大小
     * @param exceptionSleep      异常后休眠时间（秒）
     * @param maxRetryCount       最大重试次数
     */
    public KafkaTxMsgHandler(KafkaProducer<String, String> kafkaProducer,
                             TxMsgSqlStore txMsgSqlStore,
                             int fetchLimit,
                             int maxResendNumPerTask,
                             int deleteBatchSize,
                             int exceptionSleep,
                             int maxRetryCount) {
        super(deleteBatchSize, txMsgSqlStore, fetchLimit, maxResendNumPerTask, exceptionSleep, maxRetryCount);
        Assert.notNull(kafkaProducer, "KafkaProducer cannot be null");
        this.kafkaProducer = kafkaProducer;
    }

    /**
     * 简化构造器（使用默认配置）
     * <p>
     * 默认值：fetchLimit=50, maxResendNumPerTask=2000, deleteBatchSize=200,
     * exceptionSleep=5s, maxRetryCount=5
     */
    public KafkaTxMsgHandler(KafkaProducer<String, String> kafkaProducer,
                             TxMsgSqlStore txMsgSqlStore) {
        this(kafkaProducer, txMsgSqlStore, 50, 2000, 200, 5, 5);
    }

    /**
     * 单条消息发送（异步回调）
     * <p>
     * 发送成功更新状态为已发送，发送失败抛出异常由上层处理。
     */
    @Override
    protected void sendMessage(TxMsgModel txMsgModel) {
        String topic = txMsgModel.getTopic();
        String msgKey = txMsgModel.getMsgKey();
        String content = txMsgModel.getContent();
        Long msgId = txMsgModel.getId();
        try {
            ProducerRecord<String, String> record = new ProducerRecord<>(topic, msgKey, content);
            kafkaProducer.send(record, (metadata, exception) -> {
                if (exception == null) {
                    logger.debug("消息发送成功 [msgId: {}, topic: {}, partition: {}, offset: {}]",
                            msgId, metadata.topic(), metadata.partition(), metadata.offset());
                    int updateRows = txMsgSqlStore.updateSendMsg(txMsgModel);
                    if (updateRows <= 0) {
                        logger.warn("消息状态更新失败，记录不存在 [msgId: {}]", msgId);
                    }
                }
            });
        } catch (Exception e) {
            String errorMsg = String.format("消息发送失败 [msgId: %s, topic: %s", msgId, topic);
            throw new TxMsgException(errorMsg, e);
        }
    }

    /**
     * 批量消息发送
     * <p>
     * 通过Kafka异步回调收集发送结果，然后批量更新成功消息的状态，
     * 失败消息累加重试次数。
     */
    @Override
    protected void batchSendMessages(List<TxMsgModel> txMsgModels) {
        if (ListUtils.isEmpty(txMsgModels)) {
            return;
        }
        final List<Long> successMsgIds = Collections.synchronizedList(new ArrayList<>(txMsgModels.size()));
        final List<Long> failedMsgIds = Collections.synchronizedList(new ArrayList<>());

        txMsgModels.forEach(model -> {
            ProducerRecord<String, String> record = new ProducerRecord<>(model.getTopic(), model.getMsgKey(), model.getContent());
            kafkaProducer.send(record, (metadata, exception) -> {
                if (exception == null) {
                    logger.debug("批量消息发送成功 [msgId: {}, topic: {}]", model.getId(), model.getTopic());
                    successMsgIds.add(model.getId());
                } else {
                    logger.error("批量消息发送失败 [msgId: {}, topic: {}]", model.getId(), model.getTopic(), exception);
                    failedMsgIds.add(model.getId());
                }
            });
        });

        // 等待所有消息发送完毕
        kafkaProducer.flush();

        // 失败消息累加重试次数
        for (Long failedMsgId : failedMsgIds) {
            txMsgSqlStore.incrementRetryCount(failedMsgId);
        }

        // 批量更新成功消息状态
        if (ListUtils.isNotEmpty(successMsgIds)) {
            int updateRows = txMsgSqlStore.batchUpdateSendMsg(successMsgIds);
            logger.info("批量更新消息状态完成, 应更新: {}, 实际更新: {}", successMsgIds.size(), updateRows);
            if (updateRows != successMsgIds.size()) {
                logger.warn("部分消息状态更新失败, 预期: {}, 实际: {}", successMsgIds.size(), updateRows);
            }
        }
    }
}
