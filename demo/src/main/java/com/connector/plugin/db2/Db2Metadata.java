package com.connector.plugin.db2;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import io.trino.spi.StandardErrorCode;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.ConnectorMetadata;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTableMetadata;
import io.trino.spi.connector.ConnectorTableVersion;
import io.trino.spi.connector.Constraint;
import io.trino.spi.connector.ConstraintApplicationResult;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.type.BigintType;
import io.trino.spi.type.DateType;
import io.trino.spi.type.DoubleType;
import io.trino.spi.type.IntegerType;
import io.trino.spi.type.RealType;
import io.trino.spi.type.SmallintType;
import io.trino.spi.type.TimeType;
import io.trino.spi.type.TimestampType;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarcharType;

public class Db2Metadata implements ConnectorMetadata{

    private final Db2ConnectionPool connectionPool;

    public Db2Metadata(Db2ConnectionPool connectionPool){
        this.connectionPool = connectionPool;
    }

    @Override
    public List<String> listSchemaNames (ConnectorSession Session) {
        List<String> schemaNames = new ArrayList<>();

        try(Connection connection = connectionPool.getConnection();
           Statement statement = connection.createStatement();
           ResultSet rs = statement.executeQuery("SELECT SCHEMANAME FROM SYSCAT.SCHEMATA")) {

            while (rs.next()) {
                String schemaName = rs.getString(1);
                schemaNames.add(schemaName.trim().toLowerCase());
            }
        } catch (Exception e) {
            throw new TrinoException(StandardErrorCode.GENERIC_INTERNAL_ERROR, "Falha ao listar schemas do DB2: " + e.getMessage(), e);
        }

        return schemaNames;
    }

    @Override
    public List<SchemaTableName> listTables(ConnectorSession session, Optional<String> schemaName) {
        List<SchemaTableName> tableNames = new ArrayList<>();
        StringBuilder sqlBuilder = new StringBuilder("SELECT TABSCHEMA, TABNAME FROM SYSCAT.TABLES");

        if (schemaName.isPresent()){
            sqlBuilder.append(" WHERE TABSCHEMA = UPPER(?)");
        }
        String sql = sqlBuilder.toString();

        try(Connection connection = connectionPool.getConnection();
            PreparedStatement statement = connection.prepareStatement(sql)) {

            if (schemaName.isPresent()){
                statement.setString(1, schemaName.get());
            }

            try (ResultSet rs = statement.executeQuery()) {
                while (rs.next()) {
                    String dbSchemaName = rs.getString("TABSCHEMA");
                    String dbTableName = rs.getString("TABNAME");

                    tableNames.add(new SchemaTableName(
                        dbSchemaName.trim().toLowerCase(),
                        dbTableName.trim().toLowerCase()
                    ));
                }
            }
        } catch (SQLException e) {
            String schemaDescription = schemaName.orElse("todos os esquemas");
            throw new TrinoException(StandardErrorCode.GENERIC_INTERNAL_ERROR, "Falha ao listar tabelas do schema: " + schemaDescription + " no DB2 " + e.getMessage(), e);
        }

        return tableNames;
    }
    
@Override
public Optional<ConstraintApplicationResult<ConnectorTableHandle>> applyFilter(
        ConnectorSession session,
        ConnectorTableHandle table,
        Constraint constraint) {
    
    Db2TableHandle handle = (Db2TableHandle) table;
    
    TupleDomain<ColumnHandle> oldDomain = handle.getConstraint();
    TupleDomain<ColumnHandle> newDomain = constraint.getSummary();
    
    TupleDomain<ColumnHandle> combinedDomain = oldDomain.intersect(newDomain);
    
    System.out.println("Combined Domain: " + combinedDomain);
    
    if (combinedDomain.equals(oldDomain)) {
        System.out.println("Domains são iguais - retornando empty");
        return Optional.empty();
    }
    
    // Cria um novo handle com os predicados
    Db2TableHandle newHandle = new Db2TableHandle(
        handle.getSchemaTableName(),
        combinedDomain
    );
    
    System.out.println("Retornando novo handle com constraint");
    
    return Optional.of(new ConstraintApplicationResult<>(
        newHandle,
        TupleDomain.all(), // Remaining constraint
        constraint.getExpression(),
        false // precalculateStatistics
    ));
}

    @Override
    public ConnectorTableHandle getTableHandle(ConnectorSession session, SchemaTableName tableName,
            Optional<ConnectorTableVersion> startVersion, Optional<ConnectorTableVersion> endVersion) {
    
        if (startVersion.isPresent() || endVersion.isPresent()){
            throw new TrinoException(StandardErrorCode.NOT_SUPPORTED, "Este conector DB2 não suporta 'time travel' (consultas por versão)");
        }
        
        String sql = "SELECT 1 FROM SYSCAT.TABLES WHERE TABSCHEMA = UPPER(?) AND TABNAME = UPPER(?)";
        
        try (Connection connection = connectionPool.getConnection();
             PreparedStatement statement = connection.prepareStatement(sql)) {
            
            statement.setString(1, tableName.getSchemaName());
            statement.setString(2, tableName.getTableName());
            
            try (ResultSet rs = statement.executeQuery()) {
                if (rs.next()) {
                    return new Db2TableHandle(tableName);
                }
            }
        } catch (SQLException e) {
            throw new TrinoException(
                StandardErrorCode.GENERIC_INTERNAL_ERROR,
                "Erro ao verificar tabela: " + e.getMessage(),
                e
            );
        }
        return null;
    }

    @Override
    public Map<String, ColumnHandle> getColumnHandles(ConnectorSession session, ConnectorTableHandle tableHandle) {
        ConnectorTableMetadata tableMetadata = getTableMetadata(session, tableHandle);

        Map<String, ColumnHandle> columnHandles = new HashMap<>();
        for (ColumnMetadata column : tableMetadata.getColumns()) {
            columnHandles.put(column.getName(), new Db2ColumnHandle(column.getName(), column.getType()));
        }
        return columnHandles;
    }

    @Override
    public ConnectorTableMetadata getTableMetadata(ConnectorSession session, ConnectorTableHandle table) {
        
        Db2TableHandle db2TableHandle = (Db2TableHandle) table;
        SchemaTableName schemaTableName = db2TableHandle.getSchemaTableName();
        String schemaName = schemaTableName.getSchemaName();
        String tableName = schemaTableName.getTableName();

        List<ColumnMetadata> columns = new ArrayList<>();

        String sql = "SELECT COLNAME, TYPENAME FROM SYSCAT.COLUMNS WHERE TABSCHEMA = UPPER(?) AND TABNAME = UPPER(?) ORDER BY COLNO";

        try (Connection connection = connectionPool.getConnection();
             PreparedStatement statement = connection.prepareStatement(sql)) {
            
            statement.setString(1, schemaName);
            statement.setString(2, tableName);

            try (ResultSet rs = statement.executeQuery()) {
                while (rs.next()) {
                    String colName = rs.getString("COLNAME").trim().toLowerCase();
                    String dbTypeName = rs.getString("TYPENAME");
                    Type trinoType = toTrinoType(dbTypeName);

                    if (trinoType != null){
                        columns.add(new ColumnMetadata(colName, trinoType));
                    }
                }

            }        
        } catch (SQLException e) {
            throw new TrinoException(StandardErrorCode.GENERIC_INTERNAL_ERROR,"Falha ao obter metadados da tabela " + schemaName + "." + tableName + ": " + e.getMessage(), e);
        }

        return new ConnectorTableMetadata(schemaTableName, columns);
    }

    @Override
    public ColumnMetadata getColumnMetadata(ConnectorSession session, ConnectorTableHandle tableHandle, ColumnHandle columnHandle) {
        Db2ColumnHandle db2ColumnHandle = (Db2ColumnHandle) columnHandle;

        return new ColumnMetadata(db2ColumnHandle.getColumnName(), db2ColumnHandle.getColumnType());
    }

    private Type toTrinoType(String dbTypeName) {
        if (dbTypeName == null) {
            return null;
        }

        String normalizedType = dbTypeName.toUpperCase().trim();

        if (normalizedType.startsWith("VARCHAR") ||
            normalizedType.startsWith("CHARACTER") ||
            normalizedType.startsWith("CHAR") ||
            normalizedType.startsWith("LONG VARCHAR") ||
            normalizedType.startsWith("CLOB") ||
            normalizedType.equals("GRAPHIC") ||
            normalizedType.equals("VARGRAPHIC")) {
            return VarcharType.VARCHAR;
        }
        if (normalizedType.startsWith("DECIMAL") || normalizedType.startsWith("NUMERIC")) {
            return DoubleType.DOUBLE;
        }
        if (normalizedType.startsWith("TIMESTAMP")) {
            return TimestampType.TIMESTAMP_MILLIS;
        }
        switch (normalizedType) {
            case "INTEGER":
            case "INT":
                return IntegerType.INTEGER;
            case "SMALLINT":
                return SmallintType.SMALLINT;
            case "BIGINT":
                return BigintType.BIGINT;
            case "REAL":
                return RealType.REAL;
            case "DOUBLE":
            case "FLOAT":
                return DoubleType.DOUBLE;
            case "DATE":
                return DateType.DATE;
            case "TIME":
                return TimeType.TIME_MILLIS;
            default:
                System.err.println("Tipo DB2 não mapeado: " + dbTypeName);
                return VarcharType.VARCHAR; // Fallback para VARCHAR
        }
    }
}