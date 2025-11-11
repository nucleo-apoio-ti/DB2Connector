package com.connector.plugin.db2;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.LocalDate;
import java.time.LocalTime;
import java.util.List;
import java.util.stream.Collectors;

import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.trino.spi.connector.RecordCursor;
import io.trino.spi.connector.SchemaTableName;
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

public class Db2RecordCursor implements RecordCursor{

    private final List<Db2ColumnHandle> columns;
    private final Connection connection;
    private final PreparedStatement statement;
    private final ResultSet resultSet;
    
    public Db2RecordCursor(Db2ConnectionPool connectionPool, SchemaTableName schemaTableName, List<Db2ColumnHandle> columns) {
        this.columns = columns;

        try {
            this.connection = connectionPool.getConnection();

            String sql = buildSql(schemaTableName, columns);
            
            this.statement = connection.prepareStatement(sql);
            this.resultSet = statement.executeQuery();

        } catch (SQLException e) {
            // Se falhar, fecha tudo e lança um erro
            close();
            throw new RuntimeException("Falha ao executar a consulta no DB2: " + e.getMessage(), e);
        }
    }

    private String buildSql(SchemaTableName schemaTableName, List<Db2ColumnHandle> columns) {
        StringBuilder sqlBuilder = new StringBuilder("SELECT ");

        String columnNames = columns.stream()
                .map(Db2ColumnHandle::getColumnName) 
                .map(String::toUpperCase)
                .map(name -> "\"" + name + "\"")
                .collect(Collectors.joining(", "));
        
        sqlBuilder.append(columnNames);
        
        sqlBuilder.append(" FROM ")
                  // O Trino dá-nos os nomes normalizados (minúsculas)
                  // O DB2 espera maiúsculas (UPPER)
                  .append(schemaTableName.getSchemaName().toUpperCase())
                  .append(".")
                  .append(schemaTableName.getTableName().toUpperCase());
        
        return sqlBuilder.toString();
    }

    @Override
    public long getCompletedBytes() {
        return 0; 
    }

    @Override
    public long getReadTimeNanos() {
        return 0;
    }

    @Override
    public Type getType(int field) {
        return columns.get(field).getColumnType();
    }

    @Override
    public boolean advanceNextPosition() {
        try {
            return resultSet.next();
        } catch (SQLException e) {
            throw new RuntimeException("Erro ao avançar o cursor do DB2: " + e.getMessage(), e);
        }
    }

    @Override
    public long getLong(int field) {
        try {
            return resultSet.getLong(field + 1);
        } catch (SQLException e) {
            throw new RuntimeException("Erro ao ler o campo long (índice " + field + ")", e);
        }
    }
    
    @Override
    public boolean getBoolean(int field) {
        try {
            return resultSet.getBoolean(field + 1);
        } catch (SQLException e) {
            throw new RuntimeException("Erro ao ler o campo boolean (índice " + field + ")", e);
        }
    }

    @Override
    public double getDouble(int field) {
        try {
            return resultSet.getDouble(field + 1);
        } catch (SQLException e) {
            throw new RuntimeException("Erro ao ler o campo double (índice " + field + ")", e);
        }
    }

    @Override
    public Slice getSlice(int field) {
        try {
            String value = resultSet.getString(field + 1);
            if (value == null) {
                return Slices.EMPTY_SLICE;
            }
            return Slices.utf8Slice(value);
        } catch (SQLException e) {
            throw new RuntimeException("Erro ao ler o campo Slice/String (índice " + field + ")", e);
        }
    }

    @Override
    public boolean isNull(int field) {
        try {
            return resultSet.getObject(field + 1) == null;
        } catch (SQLException e) {
            throw new RuntimeException("Erro ao verificar nulidade (índice " + field + ")", e);
        }
    }

    public Object getObject(int field) {
        try {
            Type type = getType(field);
                        
            if (type.equals(IntegerType.INTEGER) || type.equals(SmallintType.SMALLINT)) {
                return resultSet.getInt(field + 1);
            }
            if (type.equals(BigintType.BIGINT)) {
                return resultSet.getLong(field + 1);
            }
            if (type.equals(DoubleType.DOUBLE) || type.equals(RealType.REAL)) {
                return resultSet.getDouble(field + 1);
            }
            if (type.equals(VarcharType.VARCHAR)) {
                return resultSet.getString(field + 1);
            }
            if (type.equals(DateType.DATE)) {
                LocalDate date = resultSet.getObject(field + 1, LocalDate.class);
                return (date == null) ? null : date.toEpochDay();
            }
            if (type.equals(TimeType.TIME_MILLIS)) {
                LocalTime time = resultSet.getObject(field + 1, LocalTime.class);
                return (time == null) ? null : time.toNanoOfDay() * 1000;
            }
            if (type.equals(TimestampType.TIMESTAMP_MILLIS)) {
                return resultSet.getTimestamp(field + 1).getTime();
            }

            // Fallback
            return resultSet.getObject(field + 1);

        } catch (SQLException e) {
            throw new RuntimeException("Erro ao ler o campo Object (índice " + field + ")", e);
        }
    }

    @Override
    public void close() {
        try {
            if (resultSet != null) {
                resultSet.close();
            }
            if (statement != null) {
                statement.close();
            }
            if (connection != null) {
                connection.close();
            }
        } catch (SQLException e) {
            // Ignora erros ao fechar
        }
    }
}
