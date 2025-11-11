package com.connector.plugin.db2;

import java.util.List;
import java.util.stream.Collectors;

import io.trino.spi.connector.RecordCursor;
import io.trino.spi.connector.RecordSet;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.type.Type;

public class Db2RecordSet implements RecordSet{

    private final Db2ConnectionPool connectionPool;
    private final SchemaTableName schemaTableName;
    private final List<Db2ColumnHandle> columns;
    private final List<Type> columnTypes;

    public Db2RecordSet(Db2ConnectionPool connectionPool, SchemaTableName schemaTableName, List<Db2ColumnHandle> columns){
        this.connectionPool = connectionPool;
        this.schemaTableName = schemaTableName;
        this.columns = columns;
        this.columnTypes = columns.stream().map(Db2ColumnHandle::getColumnType).collect(Collectors.toList());
    }

    @Override
    public List<Type> getColumnTypes() {
        return columnTypes;
    }

    @Override
    public RecordCursor cursor() {
        return new Db2RecordCursor(connectionPool, schemaTableName, columns);
    }
}
