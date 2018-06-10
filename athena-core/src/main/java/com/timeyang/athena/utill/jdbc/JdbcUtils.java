package com.timeyang.athena.utill.jdbc;

import com.timeyang.athena.AthenaException;
import com.timeyang.athena.utill.Asserts;
import com.timeyang.athena.utill.ReflectionUtils;
import com.timeyang.athena.utill.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.sql.*;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

/**
 * @author https://github.com/chaokunyang
 */
public class JdbcUtils {

    private static final Logger LOGGER = LoggerFactory.getLogger(JdbcUtils.class);

    public static List<String> getAllTables(Connection connection) {
        List<String> tables = new ArrayList<>();
        try {
            DatabaseMetaData md = connection.getMetaData();
            try (ResultSet rs = md.getTables(null, null, "%", null)) {
                while (rs.next()) {
                    tables.add(rs.getString(3));
                }
            }
        } catch (SQLException e) {
            LOGGER.error("Can't get all tables", e);
            throw new AthenaException("Can't get all tables", e);
        }
        return tables;
    }

    public static boolean isTableExists(Connection connection, String tableName) {
        return getAllTables(connection).contains(tableName.toUpperCase());
    }

    /**
     * create table if table absent
     *
     * @param dataSource DataSource
     * @param tableName  table name
     * @param tableSql   table ddl sql
     * @return true if table created or false if table exists
     */
    public static boolean createTableIfAbsent(DataSource dataSource, String tableName, String tableSql) {
        try (Connection connection = dataSource.getConnection()) {
            return createTableIfAbsent(connection, tableName, tableSql);
        } catch (SQLException e) {
            throw new AthenaException("Can't get connection", e);
        }
    }

    /**
     * create table if table absent
     *
     * @param connection Connection
     * @param tableName  table name
     * @param tableSql   table ddl sql
     * @return true if table created or false if table exists
     */
    public static boolean createTableIfAbsent(Connection connection, String tableName, String tableSql) {
        if (!isTableExists(connection, tableName)) {
            try (Statement statement = connection.createStatement()) {
                statement.execute(tableSql);
            } catch (SQLException e) {
                e.printStackTrace();
                String msg = String.format("Can't create table %s, table sql: %s", tableName, tableSql);
                LOGGER.error(msg, e);
                throw new AthenaException(msg, e);
            }
            return true;
        }

        return false;
    }

    public static <T> List<T> query(Connection connection, String sql, RowMapper<T> rowMapper) {
        try (Statement statement = connection.createStatement();
             ResultSet rs = statement.executeQuery(sql)) {
            List<T> results = new ArrayList<>();
            while (rs.next()) {
                int row = -1;
                try {
                    row = rs.getRow();
                } catch (SQLException ignored) {
                }
                results.add(rowMapper.mapRow(rs, row));
            }

            return results;
        } catch (SQLException e) {
            LOGGER.error(e.getMessage(), e);
            throw new AthenaException("Execute sql failed, sql: " + sql, e);
        }
    }

    public static <T> List<T> query(DataSource dataSource, String sql, RowMapper<T> rowMapper) {
        try (Connection connection = dataSource.getConnection()) {
            return query(connection, sql, rowMapper);
        } catch (SQLException e) {
            LOGGER.error(e.getMessage(), e);
            throw new AthenaException("Can't get connection", e);
        }
    }

    public static <T> PagedResult<T> queryPage(DataSource dataSource, String tableName, Page page, RowMapper<T> rowMapper) {
        long offset = page.getOffset();
        String orderByClause = page.buildOrderByClause();
        String criterionClause = page.buildCriterionClause();
        String whereClause = " ";
        if (StringUtils.hasText(criterionClause)) {
            whereClause = String.format(" where %s ", criterionClause);
        }
        String sql = String.format("select * from %s %s %s offset %d ROWS FETCH NEXT %d ROWS ONLY",
                tableName, whereClause, orderByClause, offset, page.getSize());
        try (Connection connection = dataSource.getConnection();
             Statement statement = connection.createStatement();
             ResultSet countRs = statement.executeQuery("SELECT COUNT(*) AS total FROM " + tableName)) {
            List<T> items = query(connection, sql, rowMapper);

            long total = 0;
            while (countRs.next())
                total = countRs.getLong("total");

            return new PagedResult<>(items, total, page);
        } catch (SQLException e) {
            String msg = String.format("Can't execute %s, sql: %s", tableName, sql);
            throw new AthenaException(msg, e);
        }
    }

    public static void insert(Connection connection, String tableName, List<FieldSetter> setFields) {
        insert(connection, tableName, setFields, null);
    }

    public static List<Object> insert(Connection connection, String tableName, List<FieldSetter> fieldSetters, String[] generatedIds, Class<?>... generatedIdClasses) {
        if (generatedIds != null) {
            Asserts.check(generatedIdClasses != null && generatedIdClasses.length == generatedIds.length,
                    "If generatedIds provided, generatedIdClasses must be provided too");
        }

        StringBuilder sqlBuilder = new StringBuilder("INSERT INTO ").append(tableName);
        List<FieldSetter> fields = fieldSetters.stream()
                .filter(fieldSetter -> fieldSetter.getMethodName().equals("setString") || fieldSetter.getObjects() != null)
                .collect(Collectors.toList());

        sqlBuilder.append('(');
        sqlBuilder.append(fields.stream().map(fieldSetter -> StringUtils.addUnderscores(fieldSetter.getFieldName()))
                .collect(Collectors.joining(", ")));
        sqlBuilder.append(") ");
        sqlBuilder.append("VALUES(");
        sqlBuilder.append(String.join(", ", Collections.nCopies(fields.size(), "?")));
        sqlBuilder.append(")");

        String sql = sqlBuilder.toString();
        LOGGER.debug("insert sql: {}", sql);

        PreparedStatement pStatement = null;
        try {
            if (generatedIds != null) {
                pStatement = connection.prepareStatement(sql, generatedIds);
            } else {
                pStatement = connection.prepareStatement(sql);
            }
            for (int i = 0; i < fields.size(); i++) {
                FieldSetter fieldSetter = fields.get(i);
                Object[] params = new Object[1 + fieldSetter.getObjects().length];
                params[0] = i + 1;  // index starts from 1
                System.arraycopy(fieldSetter.getObjects(), 0, params, 1, fieldSetter.getObjects().length);
                ReflectionUtils.invokeMethod(pStatement, fieldSetter.getMethodName(),
                        fieldSetter.getSignature(), (Object[]) params);
            }
            pStatement.executeUpdate();
            if (generatedIds != null) {
                List<Object> generatedList = new ArrayList<>(generatedIds.length);
                try (ResultSet generatedRs = pStatement.getGeneratedKeys()) {
                    for (int i = 0; i < generatedIds.length; i++) {
                        generatedRs.next();

                        String methodName = "get" + StringUtils.capitalize(generatedIdClasses[i].getSimpleName());
                        Object object = ReflectionUtils.invokeMethod(
                                generatedRs, methodName, new Class[]{int.class}, i + 1); // index starts from 1
                        generatedList.add(object);
                    }
                }
                return generatedList;
            } else {
                return new ArrayList<>();
            }
        } catch (SQLException e) {
            String msg = String.format("Insert into table %s failed. fieldSetters %s", tableName, fieldSetters.toString());
            LOGGER.error(msg, e);
            throw new AthenaException(msg, e);
        } finally {
            if (pStatement != null) {
                try {
                    pStatement.close();
                } catch (SQLException e) {
                    LOGGER.error(e.getMessage(), e);
                }
            }
        }
    }

}
