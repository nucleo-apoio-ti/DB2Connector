package com.connector.plugin.db2;

import io.trino.spi.connector.Connector;
import io.trino.spi.connector.ConnectorAccessControl;
import io.trino.spi.connector.ConnectorMetadata;
import io.trino.spi.connector.ConnectorRecordSetProvider;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplitManager;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.session.PropertyMetadata;
import io.trino.spi.transaction.IsolationLevel;

public class Db2Connector implements Connector {

    private final Db2ConnectionPool connectionPool;
    private final Db2Metadata metadata;
    private final Db2SplitManager splitManager;
    private final Db2RecordSetProvider recordSetProvider;

    public Db2Connector(Db2ConnectionPool connectionPool) {
        this.connectionPool = connectionPool;
        this.metadata = new Db2Metadata(connectionPool);
        this.recordSetProvider = new Db2RecordSetProvider(connectionPool);
        this.splitManager = new Db2SplitManager();
    }

    @Override
    public ConnectorTransactionHandle beginTransaction(IsolationLevel isolationLevel, boolean readOnly, boolean autoCommit) {
        return Db2TransactionHandle.INSTANCE;
    }

    @Override
    public ConnectorMetadata getMetadata(ConnectorSession session, ConnectorTransactionHandle transactionHandle) {
        return metadata;
    }

    @Override
    public ConnectorSplitManager getSplitManager() {
        return splitManager;
    }

    @Override
    public ConnectorRecordSetProvider getRecordSetProvider() {
        return recordSetProvider;
    }

    @Override
    public void shutdown() {
        if (connectionPool != null) {
            connectionPool.close();
        }
    }
}