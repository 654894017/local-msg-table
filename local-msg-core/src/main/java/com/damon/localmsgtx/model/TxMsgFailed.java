package com.damon.localmsgtx.model;

public class TxMsgFailed {
    private String msgKey;

    private String errorMsg;

    private long processTime;


    public TxMsgFailed(String msgKey, String errorMsg, Long processTime) {
        this.msgKey = msgKey;
        this.errorMsg = errorMsg;
        this.processTime = processTime;
    }

    public TxMsgFailed() {
    }

    public String getMsgKey() {
        return msgKey;
    }

    public void setMsgKey(String msgKey) {
        this.msgKey = msgKey;
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
