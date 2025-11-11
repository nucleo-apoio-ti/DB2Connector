package com.connector.plugin.db2;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.SchemaTableName;

public class Db2TableHandle implements ConnectorTableHandle {
    
    private final SchemaTableName schemaTableName;

    @JsonCreator
    public Db2TableHandle(@JsonProperty("schemaTableName") SchemaTableName schemaTableName) {
        this.schemaTableName = schemaTableName;
    }

    @JsonProperty
    public SchemaTableName getSchemaTableName() {
        return this.schemaTableName;
    }
}