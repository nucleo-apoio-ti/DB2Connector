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

import com.google.common.collect.ImmutableList;

import io.trino.spi.StandardErrorCode;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.AggregateFunction;
import io.trino.spi.connector.AggregationApplicationResult;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.ConnectorMetadata;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTableMetadata;
import io.trino.spi.connector.ConnectorTableVersion;
import io.trino.spi.connector.Constraint;
import io.trino.spi.connector.ConstraintApplicationResult;
import io.trino.spi.connector.JoinApplicationResult;
import io.trino.spi.connector.JoinType;
import io.trino.spi.connector.LimitApplicationResult;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.connector.SortItem;
import io.trino.spi.connector.TopNApplicationResult;
import io.trino.spi.expression.Call;
import io.trino.spi.expression.ConnectorExpression;
import io.trino.spi.expression.Variable;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.type.BigintType;
import io.trino.spi.type.DateType;
import io.trino.spi.type.DoubleType;
import io.trino.spi.type.IntegerType;
import io.trino.spi.type.RealType;
import io.trino.spi.type.SmallintType;
import io.trino.spi.type.TimeType;
import io.trino.spi.type.TimestampType;
import io.trino.spi.type.TimestampWithTimeZoneType;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarcharType;

public class Db2Metadata implements ConnectorMetadata {

    private final Db2ConnectionPool connectionPool;

    public Db2Metadata(Db2ConnectionPool connectionPool) {
        this.connectionPool = connectionPool;
    }

    @Override
    public List<String> listSchemaNames(ConnectorSession Session) {
        List<String> schemaNames = new ArrayList<>();

        try (Connection connection = connectionPool.getConnection();
                Statement statement = connection.createStatement();
                ResultSet rs = statement.executeQuery("SELECT SCHEMANAME FROM SYSCAT.SCHEMATA")) {

            while (rs.next()) {
                String schemaName = rs.getString(1);
                schemaNames.add(schemaName.trim().toLowerCase());
            }
        } catch (Exception e) {
            throw new TrinoException(StandardErrorCode.GENERIC_INTERNAL_ERROR,
                    "Falha ao listar schemas do DB2: " + e.getMessage(), e);
        }

        return schemaNames;
    }

    @Override
    public List<SchemaTableName> listTables(ConnectorSession session, Optional<String> schemaName) {
        List<SchemaTableName> tableNames = new ArrayList<>();
        StringBuilder sqlBuilder = new StringBuilder("SELECT TABSCHEMA, TABNAME FROM SYSCAT.TABLES");

        if (schemaName.isPresent()) {
            sqlBuilder.append(" WHERE TABSCHEMA = UPPER(?)");
        }
        String sql = sqlBuilder.toString();

        try (Connection connection = connectionPool.getConnection();
                PreparedStatement statement = connection.prepareStatement(sql)) {

            if (schemaName.isPresent()) {
                statement.setString(1, schemaName.get());
            }

            try (ResultSet rs = statement.executeQuery()) {
                while (rs.next()) {
                    String dbSchemaName = rs.getString("TABSCHEMA");
                    String dbTableName = rs.getString("TABNAME");

                    tableNames.add(new SchemaTableName(
                            dbSchemaName.trim().toLowerCase(),
                            dbTableName.trim().toLowerCase()));
                }
            }
        } catch (SQLException e) {
            String schemaDescription = schemaName.orElse("todos os esquemas");
            throw new TrinoException(StandardErrorCode.GENERIC_INTERNAL_ERROR,
                    "Falha ao listar tabelas do schema: " + schemaDescription + " no DB2 " + e.getMessage(), e);
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
                combinedDomain,
                handle.getLimit(),
                handle.getSortOrder(),
                handle.isDistinct(),
                handle.getSqlQueryAsString());

        return Optional.of(new ConstraintApplicationResult<>(
                newHandle,
                TupleDomain.all(), // Remaining constraint
                constraint.getExpression(),
                false // precalculateStatistics
        ));
    }

    @Override
    public Optional<LimitApplicationResult<ConnectorTableHandle>> applyLimit(
            ConnectorSession session,
            ConnectorTableHandle table,
            long limit) {

        Db2TableHandle handle = (Db2TableHandle) table;

        if (handle.getLimit().isPresent() && handle.getLimit().get() <= limit) {
            return Optional.empty();
        }

        Db2TableHandle newHandle = new Db2TableHandle(
                handle.getSchemaTableName(),
                handle.getConstraint(),
                Optional.of(limit),
                handle.getSortOrder(),
                handle.isDistinct(),
                handle.getSqlQueryAsString());

        return Optional.of(new LimitApplicationResult<>(newHandle, true, false));
    }

    @Override
    public Optional<TopNApplicationResult<ConnectorTableHandle>> applyTopN(
            ConnectorSession session,
            ConnectorTableHandle table,
            long topNCount,
            List<SortItem> sortItems,
            Map<String, ColumnHandle> assignments) {

        Db2TableHandle handle = (Db2TableHandle) table;

        if (handle.getLimit().isPresent() &&
                handle.getLimit().get() <= topNCount &&
                !handle.getSortOrder().isEmpty()) {
            return Optional.empty();
        }

        List<SortItem> sortedColumnNames = new ArrayList<>();

        for (SortItem item : sortItems) {
            String symbol = item.getName();

            ColumnHandle columnHandle = assignments.get(symbol);
            if (columnHandle == null) {
                return Optional.empty();
            }

            Db2ColumnHandle db2Handle = (Db2ColumnHandle) columnHandle;
            String actualColumnName = db2Handle.getColumnName();

            sortedColumnNames.add(new SortItem(actualColumnName, item.getSortOrder()));
        }

        Db2TableHandle newHandle = new Db2TableHandle(
                handle.getSchemaTableName(),
                handle.getConstraint(),
                Optional.of(topNCount),
                sortedColumnNames,
                handle.isDistinct(),
                handle.getSqlQueryAsString());

        return Optional.of(new TopNApplicationResult<>(newHandle, true, false));
    }

    @Override
    public ConnectorTableHandle getTableHandle(ConnectorSession session, SchemaTableName tableName,
            Optional<ConnectorTableVersion> startVersion, Optional<ConnectorTableVersion> endVersion) {

        if (startVersion.isPresent() || endVersion.isPresent()) {
            throw new TrinoException(StandardErrorCode.NOT_SUPPORTED,
                    "Este conector DB2 não suporta 'time travel' (consultas por versão)");
        }

        String sql = "SELECT 1 FROM SYSCAT.TABLES WHERE TABSCHEMA = UPPER(?) AND TABNAME = UPPER(?)";

        try (Connection connection = connectionPool.getConnection();
                PreparedStatement statement = connection.prepareStatement(sql)) {

            statement.setString(1, tableName.getSchemaName());
            statement.setString(2, tableName.getTableName());

            try (ResultSet rs = statement.executeQuery()) {
                if (rs.next()) {
                    return new Db2TableHandle(tableName, null, java.util.Optional.empty(), null, false, null);
                }
            }
        } catch (SQLException e) {
            throw new TrinoException(
                    StandardErrorCode.GENERIC_INTERNAL_ERROR,
                    "Erro ao verificar tabela: " + e.getMessage(),
                    e);
        }
        return null;
    }

    @Override
    public Optional<AggregationApplicationResult<ConnectorTableHandle>> applyAggregation(
            ConnectorSession session,
            ConnectorTableHandle table,
            List<AggregateFunction> aggregates,
            Map<String, ColumnHandle> assignments,
            List<List<ColumnHandle>> groupingSets) {

        Db2TableHandle handle = (Db2TableHandle) table;

        // Suporta apenas um grouping set (GROUP BY simples)
        if (groupingSets.size() != 1) {
            return Optional.empty();
        }

        List<ColumnHandle> groupByColumns = groupingSets.get(0);

        // Valida se as funções de agregação são suportadas
        for (AggregateFunction aggregation : aggregates) {
            String functionName = aggregation.getFunctionName();
            if (!isSupportedAggregation(functionName)) {
                return Optional.empty();
            }
        }

        List<String> selectList = new ArrayList<>();
        List<String> groupByList = new ArrayList<>();

        // Listas para o resultado do Trino
        List<ConnectorExpression> projections = new ArrayList<>();
        List<io.trino.spi.connector.Assignment> assignmentsList = new ArrayList<>();
        Map<ColumnHandle, ColumnHandle> groupingColumnMapping = new HashMap<>();

        Processar Colunas de Agrupamento
        for (ColumnHandle groupCol : groupByColumns) {
            Db2ColumnHandle db2Col = (Db2ColumnHandle) groupCol;

            // Adiciona ao SQL
            selectList.add("\"" + db2Col.getColumnName().toUpperCase() + "\"");
            groupByList.add("\"" + db2Col.getColumnName().toUpperCase() + "\"");

            // Mapeia a coluna original para ela mesma (identity mapping)
            groupingColumnMapping.put(groupCol, groupCol);
        }

        // Processar Funções de Agregação
        int aggIndex = 0;
        for (AggregateFunction aggregation : aggregates) {
            String functionName = aggregation.getFunctionName();

            String argExpression = "*";
            if (!aggregation.getArguments().isEmpty()) {
                ConnectorExpression arg = aggregation.getArguments().get(0);
                if (arg instanceof Variable) {
                    String varName = ((Variable) arg).getName();
                    ColumnHandle colHandle = assignments.get(varName);
                    if (colHandle != null) {
                        Db2ColumnHandle db2Col = (Db2ColumnHandle) colHandle;
                        argExpression = "\"" + db2Col.getColumnName().toUpperCase() + "\"";
                    } else {
                        // Se não conseguir resolver a coluna argumento, aborta
                        return Optional.empty();
                    }
                }
            }

            String sqlExpression = functionName.toUpperCase() + "(" + argExpression + ")";

            String alias = "agg_" + aggIndex;
            String upperAlias = alias.toUpperCase();
            selectList.add(sqlExpression + " AS \"" + upperAlias + "\"");

            // Cria o handle da nova coluna de agregação com o alias
            Db2ColumnHandle newHandle = new Db2ColumnHandle(upperAlias, aggregation.getOutputType());

            // Adiciona à projeção de saída
            Variable variable = new Variable(upperAlias, aggregation.getOutputType());
            projections.add(variable);
            assignmentsList.add(new io.trino.spi.connector.Assignment(
                    upperAlias,
                    newHandle,
                    aggregation.getOutputType()
            ));

            aggIndex++;
        }

        // Construir SQL de Agregação
        String sourceSql = handle.getSqlQueryAsString();
        if (sourceSql == null) {
            StringBuilder baseQuery = new StringBuilder();
            baseQuery.append("SELECT * FROM \"")
                    .append(handle.getSchemaTableName().getSchemaName().toUpperCase())
                    .append("\".\"")
                    .append(handle.getSchemaTableName().getTableName().toUpperCase())
                    .append("\"");

            if (!handle.getConstraint().isAll()) {
                String whereClause = buildWhereClause(handle.getConstraint());
                if (!whereClause.isEmpty()) {
                    baseQuery.append(" WHERE ").append(whereClause);
                }
            }

            if (!handle.getSortOrder().isEmpty()) {
                baseQuery.append(" ORDER BY ");
                List<String> orderClauses = new ArrayList<>();
                for (SortItem item : handle.getSortOrder()) {
                    String direction = item.getSortOrder().isAscending() ? "ASC" : "DESC";
                    orderClauses.add("\"" + item.getName().toUpperCase() + "\" " + direction);
                }
                baseQuery.append(String.join(", ", orderClauses));
            }

            if (handle.getLimit().isPresent()) {
                baseQuery.append(" FETCH FIRST ").append(handle.getLimit().get()).append(" ROWS ONLY");
            }

            sourceSql = "(" + baseQuery.toString() + ")";
        } else {
            sourceSql = "(" + sourceSql + ")";
        }

        StringBuilder sql = new StringBuilder();
        sql.append("SELECT ").append(String.join(", ", selectList));
        sql.append(" FROM ").append(sourceSql).append(" AS T_AGG");

        if (!groupByList.isEmpty()) {
            sql.append(" GROUP BY ").append(String.join(", ", groupByList));
        }

        Db2TableHandle newTableHandle = new Db2TableHandle(
                handle.getSchemaTableName(),
                TupleDomain.all(),
                Optional.empty(),
                List.of(),
                false,
                sql.toString()
        );

        return Optional.of(new AggregationApplicationResult<ConnectorTableHandle>(
                newTableHandle,
                projections,              // Lista com APENAS as agregações
                assignmentsList,          // Lista com APENAS os assignments das agregações
                groupingColumnMapping,    // Map das colunas de agrupamento (identity mapping)
                false                     // precalculateStatistics
        ));
    }

    // Helper para validar funções suportadas
    private boolean isSupportedAggregation(String name) {
        return List.of("count", "sum", "min", "max", "avg").contains(name.toLowerCase());
    }

    // Helper para construir cláusula WHERE a partir do TupleDomain
    private String buildWhereClause(TupleDomain<ColumnHandle> constraint) {
        return "";
    }

    @Override
    public Optional<JoinApplicationResult<ConnectorTableHandle>> applyJoin(
            ConnectorSession session,
            JoinType joinType,
            ConnectorTableHandle left,
            ConnectorTableHandle right,
            ConnectorExpression joinCondition,
            Map<String, ColumnHandle> leftAssignments,
            Map<String, ColumnHandle> rightAssignments,
            io.trino.spi.connector.JoinStatistics statistics) {

        Db2TableHandle leftHandle = (Db2TableHandle) left;
        Db2TableHandle rightHandle = (Db2TableHandle) right;

        Map<ColumnHandle, ColumnHandle> leftColumnHandles = new HashMap<>();
        Map<ColumnHandle, ColumnHandle> rightColumnHandles = new HashMap<>();

        String leftSql = leftHandle.getSqlQueryAsString();
        if (leftSql == null) {
            leftSql = leftHandle.getSchemaTableName().getSchemaName() + "."
                    + leftHandle.getSchemaTableName().getTableName();
        } else {
            leftSql = "(" + leftSql + ")";
        }

        String rightSql = rightHandle.getSqlQueryAsString();
        if (rightSql == null) {
            rightSql = rightHandle.getSchemaTableName().getSchemaName() + "."
                    + rightHandle.getSchemaTableName().getTableName();
        } else {
            rightSql = "(" + rightSql + ")";
        }

        String joinTypeStr = "INNER JOIN";
        if (joinType == JoinType.LEFT_OUTER) {
            joinTypeStr = "LEFT JOIN";
        } else if (joinType == JoinType.RIGHT_OUTER) {
            joinTypeStr = "RIGHT JOIN";
        } else if (joinType == JoinType.FULL_OUTER) {
            joinTypeStr = "FULL JOIN";
        }

        StringBuilder sql = new StringBuilder();

        List<String> selectColumns = new ArrayList<>();
        int counter = 0;

        // Process Left Assignments
        for (ColumnHandle handle : leftAssignments.values()) {
            Db2ColumnHandle oldHandle = (Db2ColumnHandle) handle;
            if (leftColumnHandles.containsKey(oldHandle)) {
                continue;
            }

            // Create unique alias to avoid collisions
            String alias = "l_" + counter + "_" + oldHandle.getColumnName();
            counter++;

            selectColumns.add("L." + oldHandle.getColumnName() + " AS " + alias);

            Db2ColumnHandle newHandle = new Db2ColumnHandle(alias, oldHandle.getColumnType());
            // Map Old Handle -> New Handle
            leftColumnHandles.put(oldHandle, newHandle);
        }

        // Process Right Assignments
        for (ColumnHandle handle : rightAssignments.values()) {
            Db2ColumnHandle oldHandle = (Db2ColumnHandle) handle;
            if (rightColumnHandles.containsKey(oldHandle)) {
                continue;
            }

            // Create unique alias to avoid collisions
            String alias = "r_" + counter + "_" + oldHandle.getColumnName();
            counter++;

            selectColumns.add("R." + oldHandle.getColumnName() + " AS " + alias);

            Db2ColumnHandle newHandle = new Db2ColumnHandle(alias, oldHandle.getColumnType());
            // Map Old Handle -> New Handle
            rightColumnHandles.put(oldHandle, newHandle);
        }

        sql.append("SELECT ").append(String.join(", ", selectColumns)).append(" FROM ");

        String conditionSql = "1=1";
        if (joinCondition instanceof io.trino.spi.expression.Call) {
            conditionSql = renderExpression(joinCondition, leftAssignments, rightAssignments);
        }

        sql.append(leftSql).append(" AS L ");
        sql.append(joinTypeStr).append(" ");
        sql.append(rightSql).append(" AS R ");
        sql.append(" ON ").append(conditionSql);

        Db2TableHandle newHandle = new Db2TableHandle(
                new SchemaTableName("derived", "join_result"),
                TupleDomain.all(),
                Optional.empty(),
                List.of(),
                false,
                sql.toString());

        return Optional.of(new JoinApplicationResult<ConnectorTableHandle>(
                newHandle,
                leftColumnHandles,
                rightColumnHandles,
                false));
    }

    private String renderExpression(ConnectorExpression expression,
            Map<String, ColumnHandle> leftAssignments,
            Map<String, ColumnHandle> rightAssignments) {
        if (expression instanceof Variable) {
            String name = ((Variable) expression).getName();
            if (leftAssignments.containsKey(name)) {
                Db2ColumnHandle h = (Db2ColumnHandle) leftAssignments.get(name);
                return "L." + h.getColumnName();
            }
            if (rightAssignments.containsKey(name)) {
                Db2ColumnHandle h = (Db2ColumnHandle) rightAssignments.get(name);
                return "R." + h.getColumnName();
            }
            // Should not happen if logic is correct
            return name;
        }
        if (expression instanceof Call) {
            Call call = (Call) expression;
            String op = call.getFunctionName().getName();
            if (op.equals("equal")) {
                return renderExpression(call.getArguments().get(0), leftAssignments, rightAssignments) + " = "
                        + renderExpression(call.getArguments().get(1), leftAssignments, rightAssignments);
            }
            if (op.equals("and")) {
                return "(" + renderExpression(call.getArguments().get(0), leftAssignments, rightAssignments) + " AND "
                        + renderExpression(call.getArguments().get(1), leftAssignments, rightAssignments) + ")";
            }
        }
        // Fallback
        return "1=1";
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

                    if (trinoType != null) {
                        columns.add(new ColumnMetadata(colName, trinoType));
                    }
                }

            }
        } catch (SQLException e) {
            throw new TrinoException(StandardErrorCode.GENERIC_INTERNAL_ERROR,
                    "Falha ao obter metadados da tabela " + schemaName + "." + tableName + ": " + e.getMessage(), e);
        }

        return new ConnectorTableMetadata(schemaTableName, columns);
    }

    @Override
    public ColumnMetadata getColumnMetadata(ConnectorSession session, ConnectorTableHandle tableHandle,
            ColumnHandle columnHandle) {
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
            if (normalizedType.contains("WITH TIME ZONE")) {
                return TimestampWithTimeZoneType.TIMESTAMP_TZ_MICROS;
            }
            return TimestampType.TIMESTAMP_MICROS;
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
