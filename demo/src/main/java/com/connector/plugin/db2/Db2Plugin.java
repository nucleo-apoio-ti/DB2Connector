package com.connector.plugin.db2;

import io.trino.spi.*;
import io.trino.spi.connector.ConnectorFactory;

import java.util.List;

public class Db2Plugin implements Plugin {
    
    @Override
    public Iterable<ConnectorFactory> getConnectorFactories (){
        return List.of(new Db2ConnectorFactory());
    }

}
