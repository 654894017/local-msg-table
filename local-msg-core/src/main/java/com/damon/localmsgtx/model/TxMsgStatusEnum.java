package com.damon.localmsgtx.model;

/**
 * Transactional message status constants
 */
public enum TxMsgStatusEnum {

    WAITING(1, "Waiting to send"),
    SENT(2, "Sent"),
    CONSUMER_FAILED(3, "Consumer failed"),
    CONSUMER_SUCCESS(4, "Consumer success");

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
