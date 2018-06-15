package com.timeyang.athena.task;

import com.timeyang.athena.task.TaskInfo.FinishedTask;
import com.timeyang.athena.task.TaskInfo.RunningTask;
import com.timeyang.athena.task.TaskInfo.WaitingTask;
import com.timeyang.athena.task.exec.TaskFactory;
import com.timeyang.athena.util.jdbc.Page;
import com.timeyang.athena.util.jdbc.PagedResult;

import java.util.List;
import java.util.Map;
import java.util.concurrent.Future;

/**
 * @author https://github.com/chaokunyang
 */
public interface TaskManager {

    void start();

    void stop();

    /**
     * use default task retry wait time
     * @param taskName task name
     * @param className task class or task factory class
     * @param params task params
     * @return task id
     */
    long submitTask(String taskName, String className, Map<String, String> params);

    /**
     * use default task retry wait time
     * @param taskName task name
     * @param factoryClass task class or task factory class
     * @param params task params
     * @return task id
     */
    long submitTask(String taskName, Class<? extends TaskFactory> factoryClass, Map<String, String> params);

    /**
     * use default task retry wait time
     * @param taskName task name
     * @param factoryClass task class or task factory class
     * @param params task params
     * @param maxTries max try number
     * @return task id
     */
    long submitTask(String taskName, Class<? extends TaskFactory> factoryClass, Map<String, String> params, int maxTries);

    /**
     * @param taskName task name
     * @param factoryClass task class or task factory class
     * @param params task params
     * @param maxTries  max try number
     * @param retryWait retry wait seconds
     * @return task id
     */
    long submitTask(String taskName, Class<? extends TaskFactory> factoryClass, Map<String, String> params, int maxTries, long retryWait);

    /**
     * if host is no specified, then host is determined according to cluster available resources, task type, server role.
     * @param task TaskInfo
     * @return taskId
     */
    long submitTask(TaskInfo task);

    Future killTask(long taskId);

    boolean isTaskAlive(long taskId);

    boolean isTaskFinished(long taskId);

    /**
     * get task logs
     * @param taskId task id
     * @param lineNumber line number start from 1
     * @param rows log rows
     * @return getLogLines
     */
    List<String> getLogLines(long taskId, int lineNumber, int rows);

    TaskInfo getTask(long taskId);

    PagedResult<WaitingTask> getWaitingTasks(Page page);

    PagedResult<RunningTask> getRunningTasks(Page page);

    PagedResult<FinishedTask> getFinishedTasks(Page page);

}
