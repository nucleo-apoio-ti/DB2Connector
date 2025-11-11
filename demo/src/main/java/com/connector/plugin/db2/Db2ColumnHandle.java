package com.connector.plugin.db2;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.type.Type;

import java.util.Objects;

public class Db2ColumnHandle implements ColumnHandle {
    
    private final String columnName;
    private final Type columnType;

    @JsonCreator
    public Db2ColumnHandle(
            @JsonProperty("columnName") String columnName,
            @JsonProperty("columnType") Type columnType) {
        this.columnName = Objects.requireNonNull(columnName, "columnName is null");
        this.columnType = Objects.requireNonNull(columnType, "columnType is null");
    }
    
    @JsonProperty
    public String getColumnName() {
        return columnName;
    }

    @JsonProperty
    public Type getColumnType() {
        return columnType;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Db2ColumnHandle that = (Db2ColumnHandle) o;
        return Objects.equals(columnName, that.columnName) &&
               Objects.equals(columnType, that.columnType);
    }

    @Override
    public int hashCode() {
        return Objects.hash(columnName, columnType);
    }

    @Override
    public String toString() {
        return "Db2ColumnHandle{" +
               "columnName='" + columnName + '\'' +
               ", columnType=" + columnType +
               '}';
    }

}