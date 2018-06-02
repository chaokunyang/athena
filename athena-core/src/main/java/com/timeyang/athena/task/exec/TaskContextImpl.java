package com.timeyang.athena.task.exec;

import com.timeyang.athena.utill.Config;
import com.timeyang.athena.utill.SerializableConfiguration;
import org.apache.hadoop.conf.Configuration;

import java.io.Serializable;

/**
 * @author https://github.com/chaokunyang
 */
public class TaskContextImpl implements TaskContext, Serializable {
    private long taskId;
    private SerializableConfiguration configuration;

    TaskContextImpl(long taskId, SerializableConfiguration configuration) {
        this.taskId = taskId;
        this.configuration = configuration;
    }

    @Override
    public long taskId() {
        return taskId;
    }

    @Override
    public Configuration hadoopConfiguration() {
        return configuration.getValue();
    }

    public static TaskContext makeTaskContext(long taskId) {
        Configuration conf = Config.newHadoopConfiguration();
        return new TaskContextImpl(taskId, new SerializableConfiguration(conf));
    }

    @Override
    public String toString() {
        return "TaskContextImpl{" +
                "taskId=" + taskId +
                ", configuration=" + hadoopConfiguration() +
                '}';
    }
}
