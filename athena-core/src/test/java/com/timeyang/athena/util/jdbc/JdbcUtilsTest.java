package com.timeyang.athena.util.jdbc;

import org.junit.Test;

import java.lang.reflect.Method;
import java.sql.PreparedStatement;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public class JdbcUtilsTest {

    @Test
    public void getPreparedStatementMethodMap() {
        List<Method> methodList = Arrays.stream(PreparedStatement.class.getDeclaredMethods())
                .filter(method -> method.getName().startsWith("set") && method.getParameterCount() == 2)
                .collect(Collectors.toList());

    }
}