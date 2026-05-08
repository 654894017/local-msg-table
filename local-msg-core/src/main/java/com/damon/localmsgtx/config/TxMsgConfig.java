package com.damon.localmsgtx.config;

import com.damon.localmsgtx.handler.AbstractTxMsgHandler;

import java.util.concurrent.ExecutorService;

/**
 * 事务消息配置类
 * <p>
 * 聚合异步发送线程池和消息处理器，作为客户端初始化的入参。
 */
public class TxMsgConfig {

    /**
     * 异步发送消息的线程池
     */
    private ExecutorService asyncSendExecutor;

    /**
     * 事务消息处理器（Kafka/RocketMQ实现）
     */
    private AbstractTxMsgHandler txMsgHandler;

    public TxMsgConfig(ExecutorService asyncSendExecutor, AbstractTxMsgHandler txMsgHandler) {
        this.asyncSendExecutor = asyncSendExecutor;
        this.txMsgHandler = txMsgHandler;
    }

    public ExecutorService getAsyncSendExecutor() {
        return asyncSendExecutor;
    }

    public void setAsyncSendExecutor(ExecutorService asyncSendExecutor) {
        this.asyncSendExecutor = asyncSendExecutor;
    }

    public AbstractTxMsgHandler getTxMsgHandler() {
        return txMsgHandler;
    }

    public void setTxMsgHandler(AbstractTxMsgHandler txMsgHandler) {
        this.txMsgHandler = txMsgHandler;
    }
}
