package com.timeyang.athena.utill;

import org.junit.Test;

import static org.junit.Assert.*;

public class FileUtilsTest {

    @Test
    public void getResourcePath() {
        System.out.println(FileUtils.getResourcePath("static/index.html"));
    }

    @Test
    public void getResourceFile() {
        System.out.println(FileUtils.getResourceFile("static/index.html"));
        System.out.println(FileUtils.getResourceFile("static/index.html").getAbsolutePath());
        System.out.println(FileUtils.getResourceFile("athena-default.properties").getAbsolutePath());
    }
}