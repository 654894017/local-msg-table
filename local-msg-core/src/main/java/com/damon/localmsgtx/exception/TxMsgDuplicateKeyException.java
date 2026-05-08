package com.damon.localmsgtx.exception;

/**
 * 消息Key重复异常
 * <p>
 * 当插入消息时msgKey已存在（违反唯一约束）时抛出。
 */
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
