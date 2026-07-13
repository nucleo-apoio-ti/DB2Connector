package com.connector.plugin.db2;

import java.util.List;
import java.util.Set;

import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplitManager;
import io.trino.spi.connector.ConnectorSplitSource;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.connector.Constraint;
import io.trino.spi.connector.FixedSplitSource;

public class Db2SplitManager implements ConnectorSplitManager {

    @Override
    public ConnectorSplitSource getSplits(
            ConnectorTransactionHandle transaction,
            ConnectorSession session,
            ConnectorTableHandle tableHandle,
            Set<ColumnHandle> dynamicFilter,
            Constraint constraint) {
        
        Db2TableHandle db2TableHandle = (Db2TableHandle) tableHandle;

        Db2Split split = new Db2Split(
            db2TableHandle.getSchemaTableName(),
            db2TableHandle.getConstraint(),
            db2TableHandle.getLimit(),
            db2TableHandle.getSortOrder(),
            db2TableHandle.isDistinct(),
            db2TableHandle.getSqlQueryAsString()
            );

        return new FixedSplitSource(List.of(split));
    }
}
