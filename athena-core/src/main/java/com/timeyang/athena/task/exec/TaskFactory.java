package com.timeyang.athena.task.exec;

import java.util.Map;

/**
 * @author https://github.com/chaokunyang
 */
public interface TaskFactory {

    Task newTask(Map<String, String> params);

}
