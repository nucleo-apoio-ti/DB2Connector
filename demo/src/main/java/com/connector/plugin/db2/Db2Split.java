package com.connector.plugin.db2;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorSplit;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.connector.SortItem;
import io.trino.spi.predicate.TupleDomain;

public class Db2Split implements ConnectorSplit {

    private final SchemaTableName schemaTableName;
    private final TupleDomain<ColumnHandle> constraint;
    private final Optional<Long> limit;
    private final List<SortItem> sortOrder;
    private boolean isDistinct;
    private final Optional<String> sqlQuery;

    // Serialization to send througt the cluster to workers
    @JsonCreator
    public Db2Split(@JsonProperty("schemaTableName") SchemaTableName schemaTableName,
            @JsonProperty("constraint") TupleDomain<ColumnHandle> constraint,
            @JsonProperty("limit") Optional<Long> limit,
            @JsonProperty("sortOrder") List<SortItem> sortOrder,
            @JsonProperty("isDistinct") boolean isDistinct,
            @JsonProperty("sqlQuery") String sqlQuery) {
        this.schemaTableName = Objects.requireNonNull(schemaTableName);
        this.constraint = constraint != null ? constraint : TupleDomain.all();
        this.sortOrder = sortOrder != null ? List.copyOf(sortOrder) : List.of();
        this.limit = limit != null ? limit : Optional.empty();
        this.isDistinct = isDistinct;
        this.sqlQuery = Optional.ofNullable(sqlQuery);
    }

    @JsonProperty
    public SchemaTableName getSchemaTableName() {
        return schemaTableName;
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
    public List<SortItem> getSortOrder() {
        return sortOrder;
    }

    @JsonProperty
    public boolean isDistinct() {
        return isDistinct;
    }

    @JsonProperty("sqlQuery")
    public String getSqlQueryAsString() {
        return this.sqlQuery.orElse(null);
    }

}
