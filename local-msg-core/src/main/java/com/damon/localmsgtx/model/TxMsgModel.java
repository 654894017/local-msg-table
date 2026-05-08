package com.damon.localmsgtx.model;

/**
 * 事务消息模型
 * <p>
 * 对应数据库事务消息表的实体映射。
 */
public class TxMsgModel {

    /**
     * 主键ID
     */
    private Long id;

    /**
     * 消息唯一标识（用于幂等性处理）
     */
    private String msgKey;

    /**
     * 消息内容（JSON格式或字符串）
     */
    private String content;

    /**
     * 消息主题
     */
    private String topic;

    /**
     * 消息标签（RocketMQ Tag过滤使用）
     */
    private String msgTag;

    /**
     * 消息状态：0-等待发送，1-已发送，2-发送失败
     *
     * @see TxMsgStatusEnum
     */
    private int status;

    /**
     * 重试次数（发送失败时累加）
     */
    private int retryCount;

    /**
     * 创建时间（毫秒时间戳）
     */
    private long createTime;

    /**
     * 更新时间（毫秒时间戳）
     */
    private Long updateTime;

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public String getContent() {
        return content;
    }

    public void setContent(String content) {
        this.content = content;
    }

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public int getStatus() {
        return status;
    }

    public void setStatus(int status) {
        this.status = status;
    }

    public String getMsgKey() {
        return msgKey;
    }

    public void setMsgKey(String msgKey) {
        this.msgKey = msgKey;
    }

    public long getCreateTime() {
        return createTime;
    }

    public void setCreateTime(long createTime) {
        this.createTime = createTime;
    }

    public Long getUpdateTime() {
        return updateTime;
    }

    public void setUpdateTime(Long updateTime) {
        this.updateTime = updateTime;
    }

    public String getMsgTag() {
        return msgTag;
    }

    public void setMsgTag(String msgTag) {
        this.msgTag = msgTag;
    }

    public int getRetryCount() {
        return retryCount;
    }

    public void setRetryCount(int retryCount) {
        this.retryCount = retryCount;
    }
}
