package com.damon.localmsgtx.model;

/**
 * Transactional message status constants
 */
public enum TxMsgStatusEnum {

    WAITING(1, "Waiting to send"),
    SENT(2, "Sent");

    private int status;

    private String desc;

    TxMsgStatusEnum(int status, String desc) {
        this.status = status;
        this.desc = desc;
    }

    public int getStatus() {
        return status;
    }
}
