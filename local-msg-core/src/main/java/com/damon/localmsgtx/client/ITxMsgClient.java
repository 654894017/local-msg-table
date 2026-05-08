package com.damon.localmsgtx.client;


import com.damon.localmsgtx.exception.TxMsgDuplicateKeyException;
import com.damon.localmsgtx.exception.TxMsgStoreException;

/**
 * 事务消息客户端接口
 * <p>
 * 提供事务消息的发送、补偿重发和过期清理能力，
 * 确保本地事务与消息发送的最终一致性。
 */
public interface ITxMsgClient {

    /**
     * 发送事务消息
     *
     * @param msgKey  消息唯一标识（不可为空，最大128字符）
     * @param content 消息内容（不可为空）
     * @return 消息ID
     * @throws IllegalArgumentException   msgKey或content为空时抛出
     * @throws TxMsgDuplicateKeyException msgKey重复时抛出
     * @throws TxMsgStoreException        消息持久化失败时抛出
     */
    Long sendTxMsg(String msgKey, String content) throws IllegalArgumentException, TxMsgDuplicateKeyException, TxMsgStoreException;

    /**
     * 发送事务消息（支持RocketMQ Tag）
     *
     * @param msgKey  消息唯一标识（不可为空，最大128字符）
     * @param msgTag  消息标签，用于RocketMQ消息过滤（最大128字符）
     * @param content 消息内容（不可为空）
     * @return 消息ID
     * @throws IllegalArgumentException   msgKey或content为空时抛出
     * @throws TxMsgDuplicateKeyException msgKey重复时抛出
     * @throws TxMsgStoreException        消息持久化失败时抛出
     */
    Long sendTxMsg(String msgKey, String msgTag, String content) throws IllegalArgumentException, TxMsgDuplicateKeyException, TxMsgStoreException;

    /**
     * 补偿重发所有待发送的消息
     * <p>
     * 用于定时任务调度，将状态为"等待发送"的消息重新投递到MQ。
     * 超过最大重试次数的消息将被标记为发送失败，不再重试。
     *
     * @throws TxMsgStoreException 查询消息失败时抛出
     */
    void resendWaitingTxMsg() throws TxMsgStoreException;

    /**
     * 清理过期的已发送消息
     * <p>
     * 删除指定时间之前已成功发送的消息记录，释放存储空间。
     *
     * @param expireTime 过期时间戳（毫秒），该时间之前的已发送消息将被删除
     * @throws TxMsgStoreException 删除消息失败时抛出
     */
    void cleanExpiredTxMsg(Long expireTime) throws TxMsgStoreException;
}
