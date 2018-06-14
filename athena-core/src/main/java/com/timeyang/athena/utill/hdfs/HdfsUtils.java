package com.timeyang.athena.utill.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;

import java.io.IOException;

/**
 * @author yangck
 */
public class HdfsUtils {

    private static final Configuration configuration = new Configuration();
    private static final FileSystem fs;

    static {
        try {
            fs = FileSystem.get(configuration);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }



}
