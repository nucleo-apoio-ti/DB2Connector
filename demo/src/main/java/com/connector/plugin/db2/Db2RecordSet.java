package com.connector.plugin.db2;

import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.RecordCursor;
import io.trino.spi.connector.RecordSet;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.connector.SortItem;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.type.Type;

public class Db2RecordSet implements RecordSet{

    private final Db2ConnectionPool connectionPool;
    private final SchemaTableName schemaTableName;
    private final List<Db2ColumnHandle> columns;
    private final List<Type> columnTypes;
    private final TupleDomain<ColumnHandle> constraint;
    private final Optional<Long> limit;
    private final List<SortItem> sortOrder;

    public Db2RecordSet(
        Db2ConnectionPool connectionPool,
        SchemaTableName schemaTableName,
        List<Db2ColumnHandle> columns,
        TupleDomain<ColumnHandle> constraint,
        Optional<Long> limit,
        List<SortItem> sortOrder
        ) {
        this.connectionPool = connectionPool;
        this.schemaTableName = schemaTableName;
        this.columns = columns;
        this.columnTypes = columns.stream().map(Db2ColumnHandle::getColumnType).collect(Collectors.toList());
        this.constraint = constraint;
        this.limit = limit;
        this.sortOrder = sortOrder;
    }

    @Override
    public List<Type> getColumnTypes() {
        return columnTypes;
    }

    @Override
    public RecordCursor cursor() {
        return new Db2RecordCursor(connectionPool, schemaTableName, columns, constraint, limit, sortOrder);
    }
}
