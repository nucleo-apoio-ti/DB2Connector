package com.connector.plugin.db2;

import java.util.Objects;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.predicate.TupleDomain;

public class Db2TableHandle implements ConnectorTableHandle {
    
    private final SchemaTableName schemaTableName;
    private final TupleDomain<ColumnHandle> constraint;

    @JsonCreator
    public Db2TableHandle(
        @JsonProperty("schemaTableName") SchemaTableName schemaTableName,
        @JsonProperty("constraint") TupleDomain<ColumnHandle> constraint) {
        this.schemaTableName = schemaTableName;
        this.constraint = constraint != null ? constraint : TupleDomain.all();
    }

    public Db2TableHandle(@JsonProperty("schemaTableName") SchemaTableName schemaTableName) {
        this(schemaTableName, TupleDomain.all());
    }

    @JsonProperty
    public SchemaTableName getSchemaTableName() {
        return this.schemaTableName;
    }

    @JsonProperty
    public TupleDomain<ColumnHandle> getConstraint() {
        return constraint;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null|| getClass() != o.getClass()) return false;
        Db2TableHandle that = (Db2TableHandle) o;
        return Objects.equals(schemaTableName, that.schemaTableName) &&
               Objects.equals(constraint, that.constraint);
    }

    @Override
    public int hashCode() {
        return Objects.hash(schemaTableName, constraint);
    }
}
