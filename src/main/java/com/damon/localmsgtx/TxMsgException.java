package com.damon.localmsgtx;

public class TxMsgException extends RuntimeException {

    public TxMsgException(String msg) {
        super(msg);
    }

    public TxMsgException(String msg, Throwable cause) {
        super(msg, cause);
    }

    public TxMsgException(Throwable cause) {
        super(cause);
    }
}