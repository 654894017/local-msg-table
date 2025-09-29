package com.damon.localmsgtx.exception;

public class TxMsgStoreException extends TxMsgException {
    public TxMsgStoreException(String msg) {
        super(msg);
    }

    public TxMsgStoreException(String msg, Throwable cause) {
        super(msg, cause);
    }

    public TxMsgStoreException(Throwable cause) {
        super(cause);
    }
}
