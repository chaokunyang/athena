package com.timeyang.athena.task.message;

import com.timeyang.athena.task.exec.Task;
import com.timeyang.athena.task.exec.TaskContext;

import java.io.Serializable;
import java.util.List;

/**
 * Message between TaskManager and TaskExecutor
 *
 * @author https://github.com/chaokunyang
 */
public abstract class TaskMessage {

    public static class ObjectMessage extends TaskMessage implements Serializable {}

    public static final class TaskSubmit extends TaskMessage implements Serializable {
        private final Task task;
        private final TaskContext taskContext;

        public TaskSubmit(Task task, TaskContext taskContext) {
            this.task = task;
            this.taskContext = taskContext;
        }

        public Task getTask() {
            return task;
        }

        public TaskContext getTaskContext() {
            return taskContext;
        }
    }

    public static final class KillTask extends TaskMessage {
    }

    public static final class StatusUpdate extends ObjectMessage {
        private final Object state;

        public StatusUpdate(Object state) {
            this.state = state;
        }

        public Object getState() {
            return state;
        }
    }

    public static final class TaskSuccess extends TaskMessage {
        private final Task task;

        public TaskSuccess(Task task) {
            this.task = task;
        }

        public Task getTask() {
            return task;
        }
    }

    public static final class TaskFailure extends ObjectMessage {
        private final Task task;
        private final Throwable throwable;

        public TaskFailure(Task task, Throwable throwable) {
            this.task = task;
            this.throwable = throwable;
        }

        public Task getTask() {
            return task;
        }

        public Throwable getThrowable() {
            return throwable;
        }
    }

    public static final class HeartBeat extends ObjectMessage {}

    public static final class LogQueryRequest extends ObjectMessage {
        private int lineNumber;
        private int rows;

        public LogQueryRequest(int lineNumber, int size) {
            this.lineNumber = lineNumber;
            this.rows = size;
        }

        public int getLineNumber() {
            return lineNumber;
        }

        public int getRows() {
            return rows;
        }

        @Override
        public String toString() {
            return "LogQueryRequest{" +
                    "lineNumber=" + lineNumber +
                    ", rows=" + rows +
                    '}';
        }
    }

    public static final class LogQueryResult extends ObjectMessage {
        private List<String> lines;

        public LogQueryResult(List<String> lines) {
            this.lines = lines;
        }

        public List<String> getLines() {
            return lines;
        }

        @Override
        public String toString() {
            return "LogQueryResult{" +
                    "lines=" + lines +
                    '}';
        }
    }

    public static class BashMessage extends ObjectMessage {
        private long taskId;
        private String script;

        public BashMessage(long taskId, String script) {
            this.taskId = taskId;
            this.script = script;
        }

        public long getTaskId() {
            return taskId;
        }

        public String getScript() {
            return script;
        }
    }

}
