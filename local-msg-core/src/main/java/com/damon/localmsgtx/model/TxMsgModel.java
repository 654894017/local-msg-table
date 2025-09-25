package com.damon.localmsgtx.model;

public class TxMsgModel {

    /**
     * 主键
     */
    private Long id;

    private String msgKey;

    /**
     * 事务消息
     */
    private String content;

    /**
     * 主题
     */
    private String topic;


    /**
     * 状态：1-等待，2-发送
     */
    private int status;

    private String randomFactor;
    /**
     * 创建时间
     */
    private long createTime;

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

    public String getRandomFactor() {
        return randomFactor;
    }

    public void setRandomFactor(String randomFactor) {
        this.randomFactor = randomFactor;
    }
}