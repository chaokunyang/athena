package com.timeyang.athena.task.exec;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.List;

public class LogInspectionTest {

    private LogInspection logInspection;

    @Before
    public void setUp() {
        logInspection = new LogInspection("./athena.log");
    }

    @Test
    public void getLines() {
        System.out.println("************************* 1~5 *************************");
        List<String> lines1 = logInspection.getLines(1, 5);
        lines1.forEach(System.out::println);

        System.out.println("************************* 201~105 *************************");
        List<String> lines2 = logInspection.getLines(201, 5);
        lines2.forEach(System.out::println);

        System.out.println("************************* 100000000~100000005 *************************");
        List<String> lines3 = logInspection.getLines(100000000, 5);
        lines3.forEach(System.out::println);

        System.out.println("************************* 501~505 *************************");
        List<String> lines4 = logInspection.getLines(501, 5);
        lines4.forEach(System.out::println);

        System.out.println("************************* indices *************************");
        List<LogIndex.Index> indices = logInspection.getLogIndex().getIndices();
        indices.forEach(System.out::println);
    }

    @After
    public void after() {
        logInspection.close();
    }
}