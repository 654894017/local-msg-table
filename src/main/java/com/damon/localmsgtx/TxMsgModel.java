package com.damon.localmsgtx;

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

    @Override
    public String toString() {
        final StringBuffer sb = new StringBuffer("TxMsgModel{");
        sb.append("id=").append(id);
        sb.append(", content='").append(content).append('\'');
        sb.append(", topic='").append(topic).append('\'');
        sb.append(", msgKey='").append(msgKey).append('\'');
        sb.append(", status=").append(status);
        sb.append(", createTime=").append(createTime);
        sb.append(", updateTime=").append(updateTime);
        sb.append('}');
        return sb.toString();
    }
}