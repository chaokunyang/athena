package com.timeyang.athena.task.exec;

import org.apache.hadoop.conf.Configuration;

/**
 * @author https://github.com/chaokunyang
 */
public interface TaskContext {

    long taskId();

    Configuration hadoopConfiguration();

}
