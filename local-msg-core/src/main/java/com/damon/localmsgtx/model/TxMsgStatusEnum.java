package com.damon.localmsgtx.model;

/**
 * Transactional message status constants
 */
public enum TxMsgStatusEnum {

    WAITING(0, "Waiting to send"),
    SENT(1, "Sent");

    private final int status;

    private final String desc;

    TxMsgStatusEnum(int status, String desc) {
        this.status = status;
        this.desc = desc;
    }

    public int getStatus() {
        return status;
    }
}
