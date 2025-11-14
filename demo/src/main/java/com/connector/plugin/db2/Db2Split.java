package com.connector.plugin.db2;

import java.util.Objects;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorSplit;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.predicate.TupleDomain;

public class Db2Split implements ConnectorSplit{

    private final SchemaTableName schemaTableName;
    private final TupleDomain<ColumnHandle> constraint;

    // Serialization to send througt the cluster to workers 
    @JsonCreator
    public Db2Split(@JsonProperty("schemaTableName") SchemaTableName schemaTableName,
                    @JsonProperty("constraint") TupleDomain<ColumnHandle> constraint) {
        this.schemaTableName = Objects.requireNonNull(schemaTableName);
        this.constraint = constraint != null ? constraint : TupleDomain.all();
    }

    @JsonProperty
    public SchemaTableName getSchemaTableName(){
        return schemaTableName;
    }

    @JsonProperty
    public TupleDomain<ColumnHandle> getConstraint() {
        return constraint;
    }
}