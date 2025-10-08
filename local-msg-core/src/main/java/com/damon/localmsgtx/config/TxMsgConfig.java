package com.damon.localmsgtx.config;

import com.damon.localmsgtx.handler.AbstractTxMsgHandler;

import java.util.concurrent.ExecutorService;

public class TxMsgConfig {

    public TxMsgConfig(ExecutorService asyncSendExecutor, AbstractTxMsgHandler txMsgHandler) {
        this.asyncSendExecutor = asyncSendExecutor;
        this.txMsgHandler = txMsgHandler;
    }

    private ExecutorService asyncSendExecutor;

    private AbstractTxMsgHandler txMsgHandler;

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