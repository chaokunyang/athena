package com.timeyang.athena.task;

import com.timeyang.athena.task.exec.Task;
import com.timeyang.athena.task.exec.TaskFactory;

import java.util.Map;

/**
 * @author yangck
 */
public class TestTaskFactory implements TaskFactory {
    @Override
    public Task newTask(Map<String, String> params) {
        return new TestTask();
    }
}
