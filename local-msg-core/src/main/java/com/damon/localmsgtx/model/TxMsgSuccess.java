package com.damon.localmsgtx.model;

public class TxMsgSuccess {
    private String msgKey;

    private long processTime;

    public TxMsgSuccess(String msgKey, long processTime) {
        this.msgKey = msgKey;
        this.processTime = processTime;
    }

    public TxMsgSuccess() {
    }

    public String getMsgKey() {
        return msgKey;
    }

    public void setMsgKey(String msgKey) {
        this.msgKey = msgKey;
    }

    public long getProcessTime() {
        return processTime;
    }

    public void setProcessTime(long processTime) {
        this.processTime = processTime;
    }
}
