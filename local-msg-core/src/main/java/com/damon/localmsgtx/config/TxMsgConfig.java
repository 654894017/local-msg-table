package com.damon.localmsgtx.config;

import com.damon.localmsgtx.handler.AbstractTxMsgHandler;
import com.damon.localmsgtx.store.TxMsgSqlStore;

import java.util.concurrent.ExecutorService;

public class TxMsgConfig {

    private TxMsgSqlStore txMsgSqlStore;

    private ExecutorService asyncSendExecutor;

    private AbstractTxMsgHandler txMsgHandler;

    public TxMsgSqlStore getTxMsgSqlStore() {
        return txMsgSqlStore;
    }

    public void setTxMsgSqlStore(TxMsgSqlStore txMsgSqlStore) {
        this.txMsgSqlStore = txMsgSqlStore;
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