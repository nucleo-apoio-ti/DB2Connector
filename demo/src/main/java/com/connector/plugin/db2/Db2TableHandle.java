package com.connector.plugin.db2;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.connector.SortItem;
import io.trino.spi.predicate.TupleDomain;

public class Db2TableHandle implements ConnectorTableHandle {
    
    private final SchemaTableName schemaTableName;
    private final TupleDomain<ColumnHandle> constraint;
    private final Optional<Long> limit;
    private final List<SortItem> sortOrder;
    private final boolean isDistinct;
    private final Optional<String> sqlQuery;

    @JsonCreator
    public Db2TableHandle(
        @JsonProperty("schemaTableName") SchemaTableName schemaTableName,
        @JsonProperty("constraint") TupleDomain<ColumnHandle> constraint,
        @JsonProperty("limit") Optional<Long> limit,
        @JsonProperty("sortOrder") List<SortItem> sortOrder,
        @JsonProperty("isDistinct") boolean isDistinct,
        @JsonProperty("sqlQuery") String sqlQuery
        ) {
        this.schemaTableName = schemaTableName;
        this.constraint = constraint != null ? constraint : TupleDomain.all();
        this.sortOrder = sortOrder != null ? List.copyOf(sortOrder) : List.of();
        this.limit = limit != null ? limit : Optional.empty();
        this.isDistinct = isDistinct;
        this.sqlQuery = Optional.ofNullable(sqlQuery);

    }

    // For simples querys(Select * from Table;)
    //public Db2TableHandle(@JsonProperty("schemaTableName") SchemaTableName schemaTableName) {
        //this(schemaTableName, TupleDomain.all());
    //}

    @JsonProperty
    public SchemaTableName getSchemaTableName() {
        return this.schemaTableName;
    }

    @JsonProperty
    public TupleDomain<ColumnHandle> getConstraint() {
        return constraint;
    }

    @JsonProperty 
    public Optional<Long> getLimit() {
        return limit;
    }

    @JsonProperty
    public boolean isDistinct() {
        return isDistinct;
    }

    public List<SortItem> getSortOrder() {
        return sortOrder;
    }

    @JsonProperty("sqlQuery")
    public String getSqlQueryAsString() {
        return this.sqlQuery.orElse(null);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null|| getClass() != o.getClass()) return false;
        Db2TableHandle that = (Db2TableHandle) o;
        return Objects.equals(schemaTableName, that.schemaTableName) &&
               Objects.equals(constraint, that.constraint) &&
               Objects.equals(sortOrder, that.sortOrder) &&
               Objects.equals(limit, that.limit) &&
               isDistinct == that.isDistinct &&
               Objects.equals(sqlQuery, that.sqlQuery);
    }

    @Override
    public int hashCode() {
        return Objects.hash(schemaTableName, constraint, limit, sortOrder, isDistinct, sqlQuery);
    }
}
