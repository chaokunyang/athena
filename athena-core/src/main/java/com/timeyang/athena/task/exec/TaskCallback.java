package com.timeyang.athena.task.exec;

/**
 * 任务执行回调
 * @author https://github.com/chaokunyang
 */
public interface TaskCallback {

    void onStarted(long taskId, int pid);

    void onSuccess(long taskId);

    void onFailure(long taskId);

    void onLost(long taskId);

}
