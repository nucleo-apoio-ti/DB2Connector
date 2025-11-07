package com.connector.plugin.db2;

import io.trino.spi.connector.*;

import java.util.Map;

public class Db2ConnectorFactory implements ConnectorFactory {

    @Override
    public String getName() {
        return "db2";
    }
    
    @Override
    public Connector create(String catalogName, Map<String, String> config, ConnectorContext context){
        Db2ConnectionPool connectionPool = new Db2ConnectionPool(config);

        return new Db2Connector(connectionPool);
    }
}
