package com.connector.plugin.db2;

import java.util.List;

import com.google.common.collect.ImmutableList;

import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorRecordSetProvider;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplit;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.connector.RecordSet;
import io.trino.spi.connector.SchemaTableName;


public class Db2RecordSetProvider implements ConnectorRecordSetProvider{
    
    private final Db2ConnectionPool connectionPool;

    public Db2RecordSetProvider(Db2ConnectionPool connectionPool) {
        this.connectionPool = connectionPool;
    }

    @Override
    public RecordSet getRecordSet(
        ConnectorTransactionHandle transaction,
        ConnectorSession session,
        ConnectorSplit split,
        ConnectorTableHandle table,
        List<? extends ColumnHandle> columns) {
        Db2Split db2Split = (Db2Split) split;
        SchemaTableName schemaTableName = db2Split.getSchemaTableName();

        ImmutableList.Builder<Db2ColumnHandle> columnHandles = ImmutableList.builder();
        for(ColumnHandle columnHandle : columns) {
            if (columnHandle != null)
                columnHandles.add((Db2ColumnHandle) columnHandle);
        }
        List<Db2ColumnHandle> db2Columns = columnHandles.build();

        return new Db2RecordSet(connectionPool, schemaTableName, db2Columns, db2Split.getConstraint(), db2Split.getLimit(), db2Split.getSortOrder());
    }
    
}
