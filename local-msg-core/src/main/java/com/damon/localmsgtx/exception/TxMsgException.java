package com.damon.localmsgtx.exception;

/**
 * 事务消息通用异常
 * <p>
 * 所有事务消息相关异常的基类。
 */
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
