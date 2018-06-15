package com.timeyang.athena.util;

import org.junit.Test;

import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public class ReflectionUtilsTest {

    @Test
    public void testMethod() {
        List<String> methodNames = Arrays.stream(ReflectionUtils.class.getDeclaredMethods())
                .map(Method::getName).collect(Collectors.toList());
        System.out.println(methodNames);
    }

}