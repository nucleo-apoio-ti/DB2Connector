package com.connector.plugin.db2;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Map;


import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

import io.trino.spi.TrinoException;
import io.trino.spi.StandardErrorCode;

public class Db2ConnectionPool implements AutoCloseable {

    private final HikariDataSource dataSource;

    /**
     * Constrói e inicializa o pool de conexões.
     *
     * @param config O mapa de configuração lido do arquivo 
     * etc/catalog/db2.properties do Trino.
     */
    public Db2ConnectionPool(Map<String, String> config) {
        HikariConfig hikariConfig = new HikariConfig();

        hikariConfig.setDriverClassName("com.ibm.db2.jcc.DB2Driver");

        String url = config.get("connection-url");
        if (url == null) {
            System.out.println("Propriedade 'connection-url' não encontrada no catálogo");
        }
        hikariConfig.setJdbcUrl(url);

        hikariConfig.setUsername(config.get("connection-user"));
        hikariConfig.setPassword(config.get("connection-password"));

        // Quantas conexões o pool deve manter no máximo.
        hikariConfig.setMaximumPoolSize(getIntConfig(config, "db2.max-pool-size", 10)); 

        // Nome para o pool
        hikariConfig.setPoolName("TrinoDb2Pool");

        hikariConfig.setAutoCommit(true); 
        try {
            this.dataSource = new HikariDataSource(hikariConfig);
        } catch (Exception e) {
            throw new TrinoException(StandardErrorCode.GENERIC_INTERNAL_ERROR, 
                "Erro ao inicializar o pool de conexões do Db2: " + e.getMessage(), e);
        }
    }

    public Connection getConnection() throws SQLException {
        return dataSource.getConnection();
    }

    @Override
    public void close() {
        if (dataSource != null) {
            dataSource.close();
        }
    }

    private int getIntConfig(Map<String, String> config, String key, int defaultValue) {
        String value = config.get(key);
        if (value != null) {
            try {
                return Integer.parseInt(value);
            } catch (NumberFormatException e) {
            }
        }
        return defaultValue;
    }
}