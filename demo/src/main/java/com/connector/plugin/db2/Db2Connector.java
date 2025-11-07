package com.connector.plugin.db2;

import io.trino.spi.connector.Connector;
import io.trino.spi.connector.ConnectorMetadata;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.transaction.IsolationLevel;

public class Db2Connector implements Connector {

    private final Db2ConnectionPool connectionPool;
    private final Db2Metadata metadata;

    public Db2Connector(Db2ConnectionPool connectionPool) {
        this.connectionPool = connectionPool;
        this.metadata = new Db2Metadata(connectionPool);
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
    public void shutdown() {
        if (connectionPool != null) {
            connectionPool.close();
        }
    }
}