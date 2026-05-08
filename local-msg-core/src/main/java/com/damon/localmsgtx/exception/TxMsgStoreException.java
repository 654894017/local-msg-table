package com.damon.localmsgtx.exception;

/**
 * 消息存储异常
 * <p>
 * 消息持久化（插入、更新、查询、删除）操作失败时抛出。
 */
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
