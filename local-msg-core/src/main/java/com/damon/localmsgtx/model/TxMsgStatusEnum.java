package com.damon.localmsgtx.model;

/**
 * 事务消息状态枚举
 */
public enum TxMsgStatusEnum {

    /**
     * 等待发送
     */
    WAITING(0, "等待发送"),

    /**
     * 已发送
     */
    SENT(1, "已发送"),

    /**
     * 发送失败（超过最大重试次数）
     */
    SEND_FAILED(2, "发送失败");

    private final int status;

    private final String desc;

    TxMsgStatusEnum(int status, String desc) {
        this.status = status;
        this.desc = desc;
    }

    public int getStatus() {
        return status;
    }

    public String getDesc() {
        return desc;
    }
}
