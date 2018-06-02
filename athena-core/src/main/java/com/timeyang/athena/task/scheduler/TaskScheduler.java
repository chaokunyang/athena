package com.timeyang.athena.task.scheduler;

import com.timeyang.athena.task.TaskInfo;

import java.util.List;
import java.util.concurrent.Future;

/**
 * @author https://github.com/chaokunyang
 */
public interface TaskScheduler {

    void start();

    void stop();

    void schedule(TaskInfo taskInfo);

    boolean isTaskRunning(long taskId);

    Future killTask(long taskId);

    List<String> getLogLines(long taskId, int lineNumber, int rows);

}
