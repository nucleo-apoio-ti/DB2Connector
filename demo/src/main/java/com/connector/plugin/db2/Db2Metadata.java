package com.connector.plugin.db2;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import io.trino.spi.connector.ConnectorMetadata;
import io.trino.spi.connector.ConnectorSession;

public class Db2Metadata implements ConnectorMetadata{

    private final Db2ConnectionPool connectionPool;

    public Db2Metadata(Db2ConnectionPool connectionPool){
        this.connectionPool = connectionPool;
    }

    @Override
    public List<String> listSchemaNames (ConnectorSession Session) {
        List<String> schemaNames = new ArrayList<>();

        try(Connection connection = connectionPool.getConnection();

           // Objeto para executar as consultas
           Statement statement = connection.createStatement();

           // Executa as consultas em si
           ResultSet rs = statement.executeQuery("SELECT SCHEMANAME FROM SYSCAT.SCHEMATA")) {

            while (rs.next()) {
                String schemaName = rs.getString(1);
                schemaNames.add(schemaName);
            }
        } catch (Exception e) {
            throw new RuntimeException("Falha ao listar esquemas do DB2: " + e.getMessage(), e);
        }

        return schemaNames;
    }
}
