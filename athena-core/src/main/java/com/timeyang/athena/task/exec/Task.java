package com.timeyang.athena.task.exec;

import java.io.Serializable;

/**
 * @author https://github.com/chaokunyang
 */
public interface Task extends Serializable {

    default void init(TaskContext ctx) {

    }

    default void exec(TaskContext ctx) {

    }

    default void onSuccess(TaskContext ctx) {

    }

    default void onError(TaskContext ctx, Throwable throwable) {

    }

    default void onLost(TaskContext ctx) {

    }

    default void onKilled(TaskContext ctx) {

    }
}
