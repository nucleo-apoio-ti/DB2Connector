package com.connector.plugin.db2;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import io.trino.spi.connector.ConnectorSplit;
import io.trino.spi.connector.SchemaTableName;

public class Db2Split implements ConnectorSplit{

    private final SchemaTableName schemaTableName;

    // Serialization to send througt the cluster to workers 
    @JsonCreator
    public Db2Split(@JsonProperty("schemaTableName") SchemaTableName schemaTableName) {
        this.schemaTableName = schemaTableName;
    }

    @JsonProperty
    public SchemaTableName getSchemaTableName(){
        return schemaTableName;
    }
}