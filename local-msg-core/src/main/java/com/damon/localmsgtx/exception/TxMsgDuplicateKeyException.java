package com.damon.localmsgtx.exception;

public class TxMsgDuplicateKeyException extends TxMsgException {
    public TxMsgDuplicateKeyException(String msg) {
        super(msg);
    }

    public TxMsgDuplicateKeyException(String msg, Throwable cause) {
        super(msg, cause);
    }

    public TxMsgDuplicateKeyException(Throwable cause) {
        super(cause);
    }
}
