package com.damon.localmsgtx.model;

import com.damon.localmsgtx.utils.StrUtil;

import java.util.Objects;

/**
 * 事务消息领域模型（充血模型）
 * <p>
 * 封装消息状态流转的业务逻辑，包括：标记已发送、累加重试次数、标记发送失败等。
 * 所有状态变更通过领域方法完成，外部只需调用 save 持久化即可。
 */
public class TxMsgModel {

    /**
     * remark最大长度
     */
    private static final int MAX_REMARK_LENGTH = 500;

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
    private long updateTime;

    /**
     * 备注（记录发送失败时的错误信息，最大500字符）
     */
    private String remark;

    /**
     * 版本号（乐观锁，由数据库自增，模型中记录查询时的版本）
     */
    private int version;

    // ==================== 工厂方法 ====================

    /**
     * 创建新的事务消息（初始状态为等待发送）
     *
     * @param id      自增主键
     * @param content 消息内容
     * @param topic   消息主题
     * @param msgKey  消息唯一标识
     * @param msgTag  消息标签
     * @return 新创建的事务消息模型
     */
    public static TxMsgModel create(Long id, String content, String topic, String msgKey, String msgTag) {
        TxMsgModel model = new TxMsgModel();
        model.id = id;
        model.content = content;
        model.topic = topic;
        model.msgKey = msgKey;
        model.msgTag = msgTag;
        model.status = TxMsgStatusEnum.WAITING.getStatus();
        model.retryCount = 0;
        model.version = 0;
        model.remark = StrUtil.EMPTY;
        long now = System.currentTimeMillis();
        model.createTime = now;
        model.updateTime = now;
        return model;
    }

    /**
     * 标记消息为已发送
     */
    public void markAsSent() {
        if (Objects.equals(TxMsgStatusEnum.SEND_FAILED.getStatus(), this.status)) {
            this.retryCount++;
        }
        this.status = TxMsgStatusEnum.SENT.getStatus();
        this.updateTime = System.currentTimeMillis();
        this.remark = StrUtil.EMPTY;
    }

    /**
     * 累加重试次数并记录失败原因
     *
     * @param failReason 失败原因
     */
    public void incrementRetry(String failReason) {
        this.retryCount++;
        this.remark = truncateRemark(failReason);
        this.updateTime = System.currentTimeMillis();
    }

    /**
     * 标记消息为发送失败
     *
     * @param failReason 失败原因
     */
    public void markAsSendFailed(String failReason) {
        this.status = TxMsgStatusEnum.SEND_FAILED.getStatus();
        this.remark = truncateRemark(failReason);
        this.updateTime = System.currentTimeMillis();
    }


    private String truncateRemark(String remark) {
        if (remark == null) {
            return null;
        }
        return remark.length() > MAX_REMARK_LENGTH ? remark.substring(0, MAX_REMARK_LENGTH) : remark;
    }

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

    public long getUpdateTime() {
        return updateTime;
    }

    public void setUpdateTime(long updateTime) {
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

    public String getRemark() {
        return remark;
    }

    public void setRemark(String remark) {
        this.remark = remark;
    }

    public int getVersion() {
        return version;
    }

    public void setVersion(int version) {
        this.version = version;
    }
}
