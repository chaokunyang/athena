package com.timeyang.athena.task;

/**
 * task state
 *
 * @author https://github.com/chaokunyang
 */
public enum TaskState {
    PENDING,
    STARTING,
    RUNNING,
    SUCCESS,
    FAILED,
    KILLED,
    LOST
}
