package com.damon.localmsgtx.model;

/**
 * 消息处理结果反馈模型
 */
public class TxMsgFeedback {
    // 原始消息Key
    private String msgKey;
    // 处理状态：成功/失败
    private boolean success;
    // 错误信息（失败时填写）
    private String errorMsg;
    // 处理时间戳
    private long processTime;

    public TxMsgFeedback(String msgKey, boolean success, String errorMsg, long processTime) {
        this.msgKey = msgKey;
        this.success = success;
        this.errorMsg = errorMsg;
        this.processTime = processTime;
    }

    public TxMsgFeedback() {
    }

    public String getMsgKey() {
        return msgKey;
    }

    public void setMsgKey(String msgKey) {
        this.msgKey = msgKey;
    }

    public boolean isSuccess() {
        return success;
    }

    public void setSuccess(boolean success) {
        this.success = success;
    }

    public String getErrorMsg() {
        return errorMsg;
    }

    public void setErrorMsg(String errorMsg) {
        this.errorMsg = errorMsg;
    }

    public long getProcessTime() {
        return processTime;
    }

    public void setProcessTime(long processTime) {
        this.processTime = processTime;
    }
}