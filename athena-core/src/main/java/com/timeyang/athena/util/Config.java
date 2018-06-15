package com.timeyang.athena.util;


import org.apache.hadoop.conf.Configuration;

/**
 * @author https://github.com/chaokunyang
 */
public class Config {

    public static Configuration newHadoopConfiguration() {
        return new Configuration();
    }

}
